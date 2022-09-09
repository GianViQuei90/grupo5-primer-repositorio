import shutil
import os

from pyannote.audio import Pipeline
pipeline = Pipeline.from_pretrained("pyannote/speaker-diarization@2022.07")

input_filename = "168316205980.wav"

# apply pretrained pipeline
diarization = pipeline(input_filename, num_speakers=2)
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
   filename = input_filename+str(speaker)+'.wav'
   filename_redact = input_filename+str(speaker)+'-redact.wav'
   shutil.copy(input_filename, filename) # copy the mono file into a new file for this channel
   command = "ffmpeg -loglevel error -y -i " + filename + " -af \"" + speaker_cmds[speaker][:-1] + "\" " + filename_redact
   print("--------------------------")
   print(command)
   os.system(command) # generate muted file except this speaker

   merge_command = merge_command + " -i " + filename_redact
   filter_command = filter_command + "[" + str(speaker_index) + ":a]"
   speaker_index = speaker_index + 1

filename_stereo = input_filename + '-stereo-pyannote.wav'
merge_command = merge_command + " -filter_complex \"" + filter_command + "amerge=inputs=" + str(len(speakers)) + "[a]\" -map \"[a]\" " + filename_stereo

print("--------------------------")
print(merge_command)
os.system(merge_command)

print("---- deleting temp files ----")
for speaker in speaker_cmds:
   filename = input_filename+str(speaker)+'.wav'
   filename_redact = input_filename+str(speaker)+'-redact.wav'
   # os.system('rm ' + filename)
   # os.system('rm ' + filename_redact)
   os.remove(filename)
   os.remove(filename_redact)