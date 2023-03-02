import boto3
import shutil
import json
import os
from pyannote.audio import Pipeline

pipeline = Pipeline.from_pretrained('pyannote/speaker-diarization@2.1', use_auth_token='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')

# Create SQS client
sqs = boto3.client('sqs','us-east-1')

# Create S3 client
s3 = boto3.client('s3')

s3r = boto3.resource('s3')

queue_url = 'https://sqs.us-east-1.amazonaws.com/477897360476/audio-converter-sqs-queue'

# Receive message from SQS queue
response = sqs.receive_message(
    QueueUrl=queue_url,
    AttributeNames=[
        'SentTimestamp'
    ],
    MaxNumberOfMessages=1,
    MessageAttributeNames=[
        'All'
    ],
    VisibilityTimeout=0,
    WaitTimeSeconds=0
)
if 'Messages' in response :
  message = response['Messages'][0]
  receipt_handle = message['ReceiptHandle']
  # Process the audio file

  body = message['Body']

  #print(body)

  # parse x:
  body_json = json.loads(body)

  filename = body_json['detail']['object']['key']

  if filename.endswith('.wav') :
    local_filename = os.path.basename(filename)

    bucketname = body_json['detail']['bucket']['name']

    print('Descargando '+filename+' desde el bucket ' + bucketname)

    with open(local_filename, 'wb') as f:
      s3.download_fileobj(bucketname, filename, f)

    input_filename = local_filename
    filename_output = '128' + input_filename
    command_128 = 'ffmpeg -i' + ' ' +filename + ' ' + '-b:a 128K' + ' ' + filename_output
    os.system(command_128)

    diarization = pipeline(filename_output, num_speakers=2)
    speakers = {}
    speaker_cmds = {}

    # print the result
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        print(f"start={turn.start:.1f}s stop={turn.end:.1f}s {speaker}")

    # dump the diarization output to disk using RTTM format
    with open("audio.rttm", "w") as rttm:
        diarization.write_rttm(rttm)

    # separate into speakers
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        if speaker not in speakers:
            speakers[speaker] = []
        speakers[speaker].append(turn)

    # generate volume strings
    for speaker in speakers:
        speaker_cmd = ""
        last_end_time = 0
        for turn in speakers[speaker]:
            speaker_cmd = speaker_cmd + "volume=enable='between(t,"+str(last_end_time)+"," + str(turn.start- 0.3) + ")':volume=0,"
            last_end_time = turn.end
        speaker_cmd = speaker_cmd + "volume=enable='between(t,"+str(last_end_time)+"," + str(10000) + ")':volume=0,"   
        speaker_cmds[speaker] = speaker_cmd


    # build the stereo file and merge them
    merge_command = "ffmpeg -y -loglevel error "
    filter_command = ""
    speaker_index = 0

    for speaker in speaker_cmds:
        filename = filename_output+str(speaker)+'.wav'
        filename_redact = filename_output+str(speaker)+'-redact.wav'
        shutil.copy(filename_output, filename) # copy the mono file into a new file for this channel
        command = "ffmpeg -loglevel error -y -i " + filename + " -af \"" + speaker_cmds[speaker][:-1] + "\" " + filename_redact
        print("--------------------------")
        print(command)
        os.system(command) # generate muted file except this speaker

        merge_command = merge_command + " -i " + filename_redact
        filter_command = filter_command + "[" + str(speaker_index) + ":a]"
        speaker_index = speaker_index + 1

    filename_stereo = filename_output + '-stereo-pyannote.wav'
    merge_command = merge_command + " -filter_complex \"" + filter_command + "amerge=inputs=" + str(len(speakers)) + "[a]\" -map \"[a]\" " + filename_stereo

    print("--------------------------")
    print(merge_command)
    os.system(merge_command)

    print("---- deleting temp files ----")
    for speaker in speaker_cmds:
        filename = filename_output+str(speaker)+'.wav'
        filename_redact = filename_output+str(speaker)+'-redact.wav'
        # os.system('rm ' + filename)
        # os.system('rm ' + filename_redact)
        os.remove(filename)
        os.remove(filename_redact)
    
    data_stereo = open(filename_stereo, 'rb')
    s3r.Bucket('postcallanalytics-inputbucket-155qbwcqu81op').put_object(Key='originalAudio/'+filename_stereo, Body=data_stereo)
    os.remove(filename_stereo)
    os.remove(input_filename)
    os.remove(filename_output)
    #Procesar archivo, la ruta está en la variable local_filename
  else :
    print('Se descarta archivo '+filename+' por que no tiene la extensión .wav')



  # Delete received message from queue
  sqs.delete_message(
      QueueUrl=queue_url,
      ReceiptHandle=receipt_handle
  )
  #print('Received and deleted message: %s' % message)
else:
  print('There is not message')