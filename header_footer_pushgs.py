def push_to_gsutil(filetogsutil):
    import os
    if os.stat(filetogsutil).st_size == 0:
        print ("no file available could not process further")
    else:
        print("happy")
def header_footer(localfilename):
    from datetime import date
    import subprocess
    today = date.today()
# dd/mm/YY
    today_date = today.strftime("%Y%m%d")
    filetogsutil = localfilename+'_'+today_date
    with open(filetogsutil,'w+') as fout:
        subprocess.call(['egrep','-v', 'H:|T:', localfilename],stdout=fout)
        fout.seek(0)
    push_to_gsutil(filetogsutil)

def validation(local_file_name):
    import subprocess
    import csv
    from datetime import date
    today = date.today()
    # dd/mm/YY
    today_date = today.strftime("%Y%m%d")
    print(today_date)
    ifile  = open(local_file_name, "r")
    reader = csv.reader(ifile,delimiter=',')
    a = next(reader)
    header = a[0].split(":")[1]
    print(header)
    b = ifile.readlines()[-1].strip()
    footer = (b.split(':')[1]).lstrip('0')
    print(footer)
    num_lines = sum(1 for line in open(local_file_name))
    # to avoid header and footer and column header
    Expected_file_record = num_lines - 3
    ifile.close()
    header = int(header)
    footer = int(footer)
    today_date = int(today_date)
    Expected_file_record = int(Expected_file_record)
    if header == today_date and footer == Expected_file_record:
        header_footer(local_file_name)
    else:
        print("error to tivoli")

def download_blob(bucket_name, data_file, local_file_name):
    from google.cloud import storage
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(data_file)
    blob.download_to_filename(local_file_name)
    print('Blob {} downloaded to {}.'.format(
        data_file,
        local_file_name))
    validation(local_file_name)

def insert_data(request):
    from google.cloud import storage
    import csv 
    import os
    bucket_name = request.args.get('bucketname')
    data_file = request.args.get('datafile')
    local_file_name = '/tmp/temp.txt'
    download_blob(bucket_name, data_file, local_file_name)
