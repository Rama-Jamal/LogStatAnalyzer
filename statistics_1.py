# Import necessary modules
from datetime import datetime, timedelta
from collections import defaultdict
import configparser
import subprocess
import time
import os

# Load configuration settings from 'config.ini' file
config = configparser.ConfigParser()
config.read('config.ini')

# Retrieve configuration settings
StatFilesPath    = config.get('General' , 'StatFilesPath')# Path to log files
FolderPath       = config.get('General' , 'FolderPath')# Path to output folder

# Function to run a shell command and capture its output
def run_command(command):
    comm = subprocess.Popen(
    command,
    shell=True,
    errors='replace',
    encoding='utf-8',
    stdout=subprocess.PIPE)
    stdout, stderr = comm.communicate()
    return stdout.strip()

# Function to analyze and gather statistics for a specific hour
def timestat(dictionaries,prev_hour,file_path,File,time_file_path):
    # Command to count events for the specified hour  
    timecomm = f"grep -c '{prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path}"
    count = run_command(timecomm)

    # Write the count to a time statistics file
    with open(time_file_path, 'a') as file:
         file.write(f'# Of Events at {prev_hour} = {count}.\n')
    
    # Command to find the peak hour with the most events
    timecomm = f"awk -F '|' '{{print $1}}' {file_path}|awk -F ' ' '{{print $2}}'|  awk -F ':' '{{print $1}}'| sort | uniq -c | sort -n | tail -1"
    peak = run_command(timecomm)
    
    # Write the peak hour to the time statistics file
    if peak != (''):
       count,Hour = peak.split(' ')
       with open(time_file_path, 'a') as file:
            file.write(f'Peak Hour = {Hour} with {count} events.\n')
    
    # Command to extract timestamps for the specified hour
    stampscomm = f"awk -F'|' '{{print $1}}' {file_path} | awk -F ' ' '{{print $2}}'| grep '{prev_hour}:[0-5][0-9]:[0-5][0-9]'",
    timestamps = run_command(stampscomm)
    timestamps = timestamps.strip().split('\n')
    
    # Process and calculate time intervals
    if timestamps != ['']:
        datetime_objects = [datetime.strptime(ts,'%H:%M:%S') for ts in timestamps]
        time_intervals   = [datetime_objects[i+1] - datetime_objects[i] for i in     range(len(datetime_objects) - 1)]
        
        # Calculate and write average, maximum, and minimum time intervals
        if len(time_intervals) != 0:
            average_interval = sum(time_intervals, timedelta()) / len(time_intervals)
            max_interval     = max(time_intervals)
            min_interval     = min(time_intervals)
            
            with open(time_file_path, 'a') as file:
              file.write(f'Average Time Interval at {prev_hour}: {average_interval}\n'+
                       f'Maximum Time Interval at {prev_hour}: {max_interval}\n'+
                       f'Minimum Time Interval at {prev_hour}: {min_interval}\n')
        
        # Categorize events into different time slots (morning, afternoon, evening, night)
        afternoon = []
        morning = []
        evening = []
        night = []

        for dt in datetime_objects:
            if 5 <= dt.hour < 12:
               morning.append(dt)
            elif 12 <= dt.hour < 17:
               afternoon.append(dt)
            elif 17 <= dt.hour < 21:
               evening.append(dt)
            else:
               night.append(dt)

        # Update dictionary with event counts for each time slot
        dictionaries[File]['morning']   += len(morning)  
        dictionaries[File]['afternoon'] += len(afternoon) 
        dictionaries[File]['evening']   += len(evening) 
        dictionaries[File]['night']     += len(night) 
        
        # Write time slot statistics to the time statistics file
        with open(time_file_path, 'a') as file:
          file.write(f'Morning Events [5-12): {dictionaries[File]["morning"]}\n'+
                     f'Afternoon Events [12-17): {dictionaries[File]["afternoon"]}\n'+
                     f'Evening Events [17-21): {dictionaries[File]["evening"]}\n'+
                     f'Night Events [21-4]: {dictionaries[File]["night"]}\n')
                        
        with open(time_file_path, 'a') as file:
                  file.write('........................\n')

# Function to gather statistics for campaigns
def camp_stat(prev_hour,file_path,File,campaigns_file_path):
    # Detect the format of CampaignID in log file
    CampaignIdFormat = run_command(f"grep 'CampaignID' {file_path} | head -n 1")
    if 'CampaignID:' in CampaignIdFormat:
       CampaignIdFormat = 'CampaignID:'
    elif 'CampaignID =' in CampaignIdFormat:
       CampaignIdFormat = 'CampaignID = '
       
    if CampaignIdFormat:
       # Command to extract and count unique CampaignIDs for the specified hour
       camp_command = f"grep '{prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path} | awk -F '{CampaignIdFormat}' '{{print $2}}' | sort -u | awk -F ')' '{{print $1}}'"
       camp = run_command(camp_command).strip().split('\n')
       
       if camp != ['']:
          with open(campaigns_file_path, 'a') as file:
               for c in camp:
                   # Count overall appearances of each CampaignID and for the specified hour
                   count = run_command(f"grep -c '{CampaignIdFormat}{c})' {file_path}")
                   file.write(f'Campaign [{c}] appears {count} times overall, ')
                   
                   count = run_command(f"grep '{prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path} | grep -co '{CampaignIdFormat}{c}'")
                   file.write(f'and {count} times at {prev_hour}.\n')
                    
          with open(campaigns_file_path, 'a') as file:
               file.write('.........................................\n')

# Function to gather statistics for offers
def offers_stat(prev_hour,file_path,File,offers_file_path):
    # Command to extract and count unique OfferTypes for the specified hour
    offers_command = f"grep '{prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path} | grep -o 'OfferType:[0-999]'|awk -F ':' '{{print $2}}' | sort -u"
    offers = run_command(offers_command).strip().split('\n')

    if offers != ['']:
       with open(offers_file_path, 'a') as file:
            for o in offers:
                # Count overall appearances of each OfferType and for the specified hour
                count = run_command(f"grep -c 'OfferType:{o})' {file_path}")
                file.write(f'Offer [{o}] appears {count} times overall, and ')
            
                count = run_command(f"grep '{Date} {prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path} |grep -c 'OfferType:{o})'")
                file.write(f'{count} times at {prev_hour}.\n')
                  
       with open(offers_file_path, 'a') as file:
            file.write('.........................................\n')    

# Function to gather statistics for messages
def messages_stat(prev_hour,file_path,File,mess_file_path):
    # Command to extract and count unique Message IDs for the specified hour
    Messages_command = f"grep '{prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path}|grep -oP 'Message = \d+,\d+,\d+,\d+,\d+'| awk -F '= ' '{{print $2}}' | sort -u"
    messages = run_command(Messages_command).strip().split('\n')

    if messages != [''] :
       with open(mess_file_path, 'a') as file:
            for m in messages:
                 # Count overall appearances of each Message ID and for the specified hour
                count = run_command(f"grep -c '{m}' {file_path}")
                file.write(f'Message [{m}] appears {count} times overall, and ')
     
                count = run_command(f"grep '{prev_hour}:[0-5][0-9]:[0-5][0-9]' {file_path} |grep -c '{m}'")
                file.write(f'{count} times at {prev_hour}.\n') 
            
       with open(mess_file_path, 'a') as file:
            file.write('.........................................\n')

# Function to retrieve log files matching the current date
def matched_files(current_date):
    files = os.listdir(StatFilesPath)
    file_names = list()
    
    for file in files:
        if f'.log{current_date}' in file:
           file_names.append(file)
    
    return file_names

# Function to perform statistics for all log files
def perform_stat(dictionaries,prev_hour,
                file_path,File,time_file_path,
                campaigns_file_path,offers_file_path,mess_file_path):
                
    timestat(dictionaries,prev_hour,file_path,File,time_file_path) 
    camp_stat(prev_hour,file_path,File,campaigns_file_path)                         
    offers_stat(prev_hour,file_path,File,offers_file_path)                         
    messages_stat(prev_hour,file_path,File,mess_file_path)

# Initialize set to keep track of processed files
proccesed_files = set()  

def main():
    dictionaries = {}
    current_hour = ''
    prev_hour = ''
    
    Flag = 0
    
    now = datetime.now()
    current_date = now.strftime("%Y%m%d")
    
    # Get log files matching the current date
    file_names = matched_files(current_date)
    
    for File in file_names:
        proccesed_files.add(File)
        dictionaries[File] = defaultdict(int)
      
        f = File.split('.')[0]
                  
        file_path = os.path.join(StatFilesPath,File)     
    
        campaigns_file_path = os.path.join(FolderPath , f'{f}{current_date}_CampStat.log')
        offers_file_path    = os.path.join(FolderPath , f'{f}{current_date}_OffStat.log')
        mess_file_path      = os.path.join(FolderPath , f'{f}{current_date}_MessStat.log')
        time_file_path      = os.path.join(FolderPath , f'{f}{current_date}_TimeStat.log')
        
        # Extract hours from the log file
        hours_in_file = run_command(f"awk -F '|' '{{print $1}}' {file_path} | awk -F ' ' '{{print $2}}' | awk -F ':' '{{print $1}}' | sort -u").strip().split('\n')
        if hours_in_file != [''] :
         if int(hours_in_file[-1]) > 1:
           for H in range(int(hours_in_file[-1])):
               prev_hour = f"{H:02d}"                        
               perform_stat(dictionaries,prev_hour,file_path,
                            File,time_file_path,campaigns_file_path,
                            offers_file_path,mess_file_path)
                            
           prev_hour = hours_in_file[-1]
           current_hour = int(hours_in_file[-1]) + 1
           current_hour = f"{current_hour:02d}"
           
         elif (hours_in_file[-1] == '01'):
            prev = int(hours_in_file[-1]) - 1
            prev_hour = f"{prev:02d}"
            perform_stat(dictionaries,prev_hour,file_path,
                         File,time_file_path,campaigns_file_path,
                         offers_file_path,mess_file_path)
                         
            prev_hour = hours_in_file[-1]
            current_hour = int(hours_in_file[-1]) + 1
            current_hour = f"{current_hour:02d}"
            
         else:
            prev_hour = '00'
            current_hour = '01'
        else:
            main()
            
    while True:
        now = datetime.now()         
        file_names = matched_files(current_date)
        
        tomorrow = datetime.strptime(current_date,"%Y%m%d")
        tomorrows_date = tomorrow+timedelta(days=1)
        tomorrows_date = tomorrows_date.strftime("%Y%m%d") 
        
        tomorrows_files = matched_files(tomorrows_date)
       
        file_path = os.path.join(StatFilesPath,file_names[0])
        hours_in_file = run_command(f"awk -F '|' '{{print $1}}' {file_path} | awk -F ' ' '{{print $2}}' | awk -F ':' '{{print $1}}' | sort -u").strip().split('\n')
        
        if (hours_in_file[-1] == current_hour) or tomorrows_files:         
          for File in file_names:   
              f = File.split('.')[0]        
              file_path = os.path.join(StatFilesPath,File)     
    
              campaigns_file_path = os.path.join(FolderPath , f'{f}{current_date}_CampStat.log')
              offers_file_path    = os.path.join(FolderPath , f'{f}{current_date}_OffStat.log')
              mess_file_path      = os.path.join(FolderPath , f'{f}{current_date}_MessStat.log')
              time_file_path      = os.path.join(FolderPath , f'{f}{current_date}_TimeStat.log')                               
              
              if File not in proccesed_files:
                 proccesed_files.add(File)
                 dictionaries[File] = defaultdict(int)
                 if int(hours_in_file[-1]) > 0:
                   for H in range(int(hours_in_file[-1])):
                     prev_hour = f"{H:02d}"                        
                     perform_stat(dictionaries,prev_hour,file_path,
                                  File,time_file_path,campaigns_file_path,
                                  offers_file_path,mess_file_path)
                   if File == file_names[-1]:
                       prev_hour = hours_in_file[-1]
                       current_hour = int(hours_in_file[-1]) + 1
                       current_hour = f"{current_hour:02d}"
                       
              else:                
                if (prev_hour != '23'):
                    perform_stat(dictionaries,prev_hour,file_path,
                                 File,time_file_path,campaigns_file_path,
                                 offers_file_path,mess_file_path)
                    
                    if File == file_names[-1]:
                       prev_hour = hours_in_file[-1]
                       current_hour = int(hours_in_file[-1]) + 1
                       current_hour = f"{current_hour:02d}"
                                 
                elif (prev_hour == '23') :
                      file_names = matched_files(current_date)
                      perform_stat(dictionaries,prev_hour,file_path,
                                   File,time_file_path,campaigns_file_path,
                                   offers_file_path,mess_file_path)
                      Flag = 1
         
        if Flag == 1:
           current_date = tomorrows_date
           prev_hour = '00'
           current_hour = '01' 
           dictionaries = {}
           Flag = 0    
                 
        time.sleep(1) 
                  
if __name__ == "__main__":
   main()                 