require 'cli_app'

class ESMappingUpdate < DatabaseCLIApplication

MAX_JOBS_COUNT = 25

  def index_name
    Cassini::Email.index_name
  end

  def client
    Cassini::Email.client
  end

  def with_retry(&block)
    while true
      begin
        block.call
        break
      rescue Curl::Err::CurlError => e
        log(:warning, "WARNING: #{e.inspect} - retrying in 10 seconds")
        log(:warning, e.backtrace.join("\n"))
        sleep(10)
        Cassini::Email.client.transport.reload_connections!
        with_retry do
          block.call
        end
      end
    end
  end


  def ts_to_suffix(ts)
    ts.strftime('%Y%m%d%H%M%S')
  end

  def create_index(name)
    log(:info, "create_index(#{name})")
    with_retry do
      Cassini::Email.create!(name)
      Chewy.wait_for_status
    end
  end

  def delete_index(name)
    log(:info, "delete_index(#{name})")
    begin
      with_retry do
        client.indices.delete(index: name)
        Chewy.wait_for_status
      end
    rescue Elasticsearch::Transport::Transport::Errors::NotFound
    end
  end

  def delete_alias(name, index)
    log(:info, "client.indices.delete_alias(name: #{name}, index: #{index})")
    with_retry do
      begin
        puts "deleting alias #{name}, index: #{index}"
        client.indices.delete_alias(name: name, index: index)
        Chewy.wait_for_status
      rescue Elasticsearch::Transport::Transport::Errors::NotFound
      end
    end
  end

  def put_alias(name, index)
    log(:info, "client.indices.put_alias(name: #{name}, index: #{index})")
    with_retry do
      client.indices.put_alias(name: name, index: index)
      Chewy.wait_for_status
    end
  end

  def wait_or_bail(process_ids, flags=0)
    begin
      pid, status = Process.wait2(-1, flags)
      puts "PID - #{pid}\tExit status - #{status}"
    rescue Errno::ECHILD
      return nil
    end
    return nil unless pid and status
    if status.exitstatus != 0
      log(:info, "Child failed: #{status.inspect} -- Bailing!")
      exit(1)
    end
    puts "Deleting process #{pid}"
    process_ids.delete(pid)
    return pid
  end

  def reap(process_ids)
    if process_ids.size >= MAX_JOBS_COUNT
      puts "number of active process #{process_ids.length}"
      wait_or_bail(process_ids)
    end
    while wait_or_bail(process_ids, Process::WNOHANG); end
  end

  def reap_all(process_ids)
    wait_or_bail(process_ids) while process_ids.size > 0
  end

  def copy_index(src, dst)
    log(:info, "copy_index(#{src}, #{dst})")
    puts "\ncopy_index(#{src}, #{dst}) - PID #{Process.pid}\n"
    r = client.search(
        index:       src,
        search_type: 'scan',
        scroll:      '5m',
        size:        100
    )
    while (r = client.scroll(scroll: '5m', body: r['_scroll_id'])) and not r['hits']['hits'].empty? do
      Cassini::Email::EmailUniversal.bulk(
          body: r['hits']['hits'].map do |data|
            {
                index: {
                    _index: dst,
                    _type:  Cassini::Email::EmailUniversal.type_name,
                    data:   data['_source']
                }
            }
          end
      )
      puts "Copying -  #{Process.pid}"
    end
  end

  def run
    job('es-mapping-update')do
      #Get all indexes
      old_indexes = Cassini::Email.indexes.sort
      new_indexes = []
      # Create a new index for writing while this runs.  We will read from
      # all data *other* than this via old alias, but include this data in
      # the overall 'emails' dataset.
      old_alias   = 'old_emails'
      delete_alias(old_alias, '*')

      Cassini::Email.indexes.each do |index|
        put_alias(old_alias, index)
      end

      Cassini::Email.create_new_writer_index
      Cassini::Email.client.transport.reload_connections!
      new_writer_index = Cassini::Email.indexes.sort.last
      puts "new writer index #{new_writer_index}"

      old_indexes.size.times do |i|
        index = old_indexes[i]
        timestamp = Time.strptime(index.split('_').last, '%Y%m%d%H%M%S')
        while Cassini::Email.client.indices.exists(index: index)
          new_timestamp = timestamp + 1.second
          if new_timestamp >= timestamp + 1.day
            raise Exception, "Out of timestamps in the day of #{timestamp}"
            exit(1)
          end
          index = index_name + '_' + ts_to_suffix(new_timestamp)
          timestamp = new_timestamp
        end
        new_indexes << index
      end

      new_indexes.uniq!
      log(:info, "Creating new indexes.\n #{new_indexes.uniq} ")

      puts "Creating new indexes.\n #{new_indexes} "
      new_indexes.each do |new_index|
        ActiveRecord::Base.establish_connection
        Cassini::Email.client.transport.reload_connections!
        create_index(new_index.split('_').last)
        puts "\nCreated "+new_index
      end

      log(:info, "Copying new old data to new indexes")
      process_ids = []
      (0..new_indexes.length-1).each do |i|
        pid = fork do
         begin
            #ActiveRecord::Base.establish_connection
            Cassini::Email.client.transport.reload_connections!
            copy_index(old_indexes[i], new_indexes[i])
            puts "\nCopied!\t-\tPID #{Process.pid}"
            delete_index(old_indexes[i])
            puts "\nIndex deleted! #{old_indexes[i]}\t-\tPID #{Process.pid}"
          ensure
            exit
         end
        end
        process_ids << pid
        puts "\n\nForked pid=#{pid} to copy #{new_indexes[i]} index."
        #log(:info, "Forked pid=#{pid} to copy #{new_indexes[i]} index.")
        reap(process_ids)
      end
      puts "# of processes #{process_ids.length}"
      #log(:info, "Finished forking workers to populate new index partitions.  Waiting for them to finish.")
      puts "Finished forking workers to populate new index partitions.  Waiting for them to finish."
      reap_all(process_ids)
      Process.waitall
      # Connections may have gotten stale by now.
      client.transport.reload_connections!
      ActiveRecord::Base.establish_connection

      log(:debug, "Deleting existing aliases.")
      puts "Deleting existing aliases"
      delete_alias(index_name, '*')

      log(:debug, "Creating new aliases.")
      puts "Creating new aliases"
      sleep(5)
      # create new aliases
      new_indexes.each do |new_index|
        put_alias(index_name, new_index)
      end
      puts  "put a new alias"
      sleep(3)
      put_alias(index_name, new_writer_index)
      log(:debug, "Done.")
      puts "DONE"
    end #End for JOB
  end #End for run method

end