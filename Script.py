#Fernando Script.py revision 5/4/23
import argparse
import pandas as pd
import os
import re
import csv
import datetime
from pymongo import MongoClient
import subprocess
from moviepy.editor import *

# Connect to MongoDB
client = MongoClient('mongodb+srv://TheFernDB:TheFern9129@cluster0.kqexskg.mongodb.net/test')
db = client['collection1']
collection1 = db['collection1']
collection2 = db['collection2']

# Parse command-line arguments
parser = argparse.ArgumentParser(description='Process Baselight, Flame, and Xytech files.')
parser.add_argument('--files', nargs='+', help='Baselight/Flame text files', required=True)
parser.add_argument('--xytech', help='Xytech file input', required=True)
parser.add_argument('--verbose', action='store_true', help='Console output on/off')
parser.add_argument('--output', choices=['csv', 'database'], help='Output to CSV or Database', required=True)
parser.add_argument('--process', metavar='<video file>', type=str, help='Path to the video file')
parser.add_argument('--export-xls', action='store_true', help='Export to XLS with new column and thumbnail')
args = parser.parse_args()
print(args)

# Define your functions to parse files, process data, and insert into MongoDB
def parse_files(file_list):
    file_data = {}
    for file in file_list:
        try:
            with open(file, 'r') as f:
                content = f.read()
            print(f"Processing file: {file}")
            print(f"File content: {content}")

            if "Baselight" in file:
                # Parse Baselight files
                file_data[file] = parse_baselight(content)
            elif "Flame" in file:
                # Parse Flame files
                file_data[file] = parse_flame(content)
            else:
                print(f"Unknown file type: {file}")
                continue
        except Exception as e:
            print(f"Error processing file: {file}, error: {e}")
            continue

    return file_data

def parse_baselight(content):
    baselight_data = re.findall(r'(/images1/.+?)(\d+(?:\s+\d+)+)', content)
    return [(match[0], match[1].split()) for match in baselight_data]

def parse_flame(content):
    flame_data = re.findall(r'(/net/flame-archive.+?)(\d+(?:\s+\d+)+)', content)
    return [(match[0], match[1].split()) for match in flame_data]

def process_data(file_data):
    processed_data = []
    for file, data in file_data.items():
        machine, user, date_str = re.match(r'(\w+)_(\w+)_(\d{8})', os.path.basename(file)).groups()
        date = datetime.datetime.strptime(date_str, "%Y%m%d").date()

        for location, frames in data:
            processed_data.append({
                "file": file,
                "machine": machine,
                "user": user,
                "date": date,
                "location": location,
                "frames": frames
            })

    return processed_data


def process_data(file_data):
    processed_data = []
    for file, data in file_data.items():
        machine, user, date_str = re.match(r'(\w+)_(\w+)_(\d{8})', os.path.basename(file)).groups()
        date = datetime.datetime.strptime(date_str, "%Y%m%d").date()

        for location, frames in data:
            # Convert date object to datetime object with zeroed time values
            datetime_obj = datetime.datetime(date.year, date.month, date.day)

            processed_data.append({
                "file": file,
                "machine": machine,
                "user": user,
                "date": datetime_obj,  # Use datetime_obj instead of date
                "location": location,
                "frames": frames
            })

    return processed_data

def frame_ranges(frames):
    ranges = []
    idx = 0  # List index
    length = 1  # Length of the current frame range

    # Initialize the first frame
    start = end = int(frames[0])
    ranges.append([start])  # Start a new range

    for frame in frames[1:]:
        frame = int(frame)
        if frame == end + 1:
            end = frame
            length += 1  # Increase the length of the current range
            ranges[idx].append(frame)
        else:
            # If the frame is not in sequence, finalize the previous range
            if (length == 1):
                ranges[idx][0] = str(ranges[idx][0])
            else:
                # Build the range between the first and last elements
                ranges[idx] = str(ranges[idx][0]) + " -> " + str(ranges[idx][-1])

            # Start a new range
            start = end = frame
            ranges.append([frame])
            idx += 1
            length = 1

    # Finalize the last range
    if (length == 1):
        ranges[idx][0] = str(ranges[idx][0])
    else:
        ranges[idx] = str(ranges[idx][0]) + " -> " + str(ranges[idx][-1])

    return ranges
def format_output(data):
    output = []
    for d in data:
        formatted_frames = ', '.join(frame_ranges(d['frames']))
        output.append(f"{d['date']} - {d['machine']} - {d['user']} - {d['location']} - {formatted_frames}")

    return output

def get_video_duration(video_file):
    cmd = f'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1 "{video_file}"'
    duration = float(subprocess.check_output(cmd, shell=True).strip())
    return duration

def convert_marks_to_timecode(marks, duration, fps):
    timecodes = []
    for mark in marks:
        time_in_seconds = float(mark) / fps  # Convert mark to float before division
        timecode = datetime.timedelta(seconds=time_in_seconds)
        timecodes.append(timecode)
    return timecodes

def create_thumbnail(video_file, time, thumbnail_size=(96, 74)):
    clip = VideoFileClip(video_file)
    thumbnail = clip.to_ImageClip(t=time).resize(thumbnail_size)
    thumbnail_path = f"thumbnail_{time}.png"
    thumbnail.save_frame(thumbnail_path)
    return thumbnail_path

def insert_into_mongodb(data):
    # Insert data into MongoDB collections
    for record in data:
        # Determine the target collection based on the machine type
        target_collection = collection1 if record["machine"] == "Baselight" else collection2

        print(f"Inserting record into collection: {target_collection.name}")
        print(f"Record: {record}")

        # Insert the record into the target collection
        result = target_collection.insert_one(record)
        print(f"Inserted ID: {result.inserted_id}")



def write_to_csv(processed_data, output_file):
    with open(output_file, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write header
        writer.writerow(['Producer', 'Operator', 'Job', 'Notes'])
        writer.writerow(['JoanJett', 'ShaneMand', 'Dirtfixing', 'Please clean files noted per Colorist DFlowers MFelix JJacobs'])

        # Write sub-header
        writer.writerow(['Show Location', 'Frames to Fix'])

        for row in processed_data:
            location = row["location"].replace("/images1", "/hpsans13/production/starwars")
            formatted_frames = ', '.join([f'{start}-{end}' for start, end in frame_ranges(row["frames"])])
            writer.writerow([location, formatted_frames])
##IGNORE THIS FOR PROJECT 3!
@def get_video_fps(video_file):
   # clip = VideoFileClip(video_file)
   # return clip.fps

# Main script
if __name__ == '__main__':
    file_data = parse_files(args.files)
    print(f"Parsed file_data: {file_data}")
    processed_data = process_data(file_data)
    print(f"Processed data: {processed_data}")

    formatted_output = format_output(processed_data)
    print(formatted_output)

    if args.process and args.export_xls:
        video_file = args.process
        video_duration = get_video_duration(video_file)
    if args.process and args.export_xls:
        video_file = args.process
        video_duration = get_video_duration(video_file)
        video_fps = get_video_fps(video_file)

        for entry in processed_data:
            mid_frame = (int(entry["frames"][0]) + int(entry["frames"][-1])) // 2
            mid_frame_time = mid_frame / video_fps
            thumbnail_path = create_thumbnail(video_file, mid_frame_time)

            # Add the thumbnail_path to the entry
            entry["thumbnail"] = thumbnail_path

            # Convert frame marks to timecodes
            timecodes = convert_marks_to_timecode(entry["frames"], video_duration, video_fps)
            entry["timecodes"] = timecodes

        # Export the data to XLS format, including the new column and thumbnails
        df = pd.DataFrame(processed_data)
        df.to_excel(args.export_xls, index=False)
       
    if args.output == 'database':
        print("Inserting data into MongoDB")
        insert_into_mongodb(processed_data)
    elif args.output == 'csv':
        output_file = 'FINALOUTPUT.csv'
        print(f"Writing data to CSV: {output_file}")
        write_to_csv(processed_data, output_file)
