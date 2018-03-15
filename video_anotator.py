#!/usr/bin/python


''' Crowdflower tool to read a video from anotated dataset
Example output:
./video_anotator.py --class interact_with_drawer
'''

import moviepy
from  moviepy.editor import VideoFileClip
import numpy as np
import imageio
import pandas
import errno
import time
import os
import re


DATA_PATH = '/media/moreaux/Data/Dataset/perso_kitchen/'
DATABASE_PATH = os.path.join(DATA_PATH, 'database')
if not os.path.isdir(DATABASE_PATH):
    os.makedirs(DATABASE_PATH)
labels = []
counter = {}


def get_args():
    '''Parse the arguments recieved'''
    # Assign description to the help doc
    parser = argparse.ArgumentParser(
        description='Crowdflower tool to read a video from anotated dataset')
    # Add arguments
    parser.add_argument(
        '-c', '--class', type=str, help='IP of your pepper',
        required=True)
    parser.add_argument(
        '-bd', '--database_path', type=str,
        help='Path where to exported database',
        required=False, default=9559)
    args = parser.parse_args()
    args.show = False if args.show.lower() == 'false' else True
    return args


def check_url(video_url):
    '''Correct a video_url if needed
    '''
    video_url = video_url.replace('YDXJ0007','kitchen_001a')
    def my_replace(match):
        match = match.group()
        h, m, s = match.split(':')
        s = int(s) + int(m) * 60 + int(h) * 60 * 60
        s = s / 30
        return '{:03d}'.format(s)

    video_url = re.sub('\d\d:\d\d:\d\d', my_replace, video_url)
    return video_url


def url_to_path(video_url):
    '''Collect all the video paths (should be performed once)
    and search for the diseired video from the url'''
    dataset_files = []
    for root, dirs, files in os.walk(DATA_PATH):
        for f in files:
            if f[-4:] == '.mp4':
                dataset_files.append(os.path.join(root, f))

    video_name = video_url.split('/')[-1]
    for f_path in dataset_files:
        if video_name in f_path:
            return f_path

    raise IOError(
        errno.ENOENT, os.strerror(errno.ENOENT), video_name)


def clean_df(df):
    '''Recieves the csv formatted as Crowdflower then:
    (1) split it with correct amount of lines
    (2) correct url if necessary'''
    initial_df_len = len(df)
    for index, row in df.iterrows():
        for i, (start, end, label) in enumerate(zip(
            row['new_phase_start'].split('\n'),
            row['new_phase_end'].split('\n'),
            row['time_action'].split('\n'))):

            new_row = row.copy()
            new_row['new_phase_start'] = start
            new_row['new_phase_end'] = end
            new_row['time_action'] = label
            new_row['video_url'] = check_url(new_row['video_url'])
            new_row['video_path'] = url_to_path(new_row['video_url'])
            df = df.append(new_row, ignore_index=True)

    df.drop(range(initial_df_len), inplace=True)
    return df


def build_dataset(dataframe):
    '''Builds a video dataset from the annotations in the dataframe.
    The database is stored in <DATABASE_PATH>
    '''
    fail_log = []
    labels = list(set(df['time_action']))
    counter = {key: 0 for key in labels}

    def _get_start_end(row):
        start = float(row['new_phase_start'])
        start = max(0., start)
        end = float(row['new_phase_end'])
        clip = VideoFileClip(row['video_path'])
        end = min(end, clip.duration)
        clip.__del__()
        return start, end

    def _export_row(row, counter):
        start, end = _get_start_end(row)
        label = row['time_action']
        clip = []  # mandatory to flush memory :/
        clip.append(VideoFileClip(row['video_path']))
        clip.append(clip[-1].subclip(start, end))
        clip.append(clip[-1].resize(height=224))
        clip_name = '{}_{:03d}.mp4'.format(label, counter[label])
        clip[-1].write_videofile(os.path.join(DATABASE_PATH, clip_name))
        for _clip in clip:  # mandatory to flush memory :/
            _clip.__del__()
        counter[label] += 1

    for idx, row in df.iterrows():
        try:
            _export_row(row, counter)
        except:
            start, end = _get_start_end(row)
            fail_log.append((idx, row['video_path'], start, end))
    
    if len(fail_log) > 0:
        print "BEWARE, some videos were not exported :"
        for fail in fail_log:
            print fail

    return labels


df = pandas.read_csv(DATA_PATH + 'annotations.csv')
df = clean_df(df)
build_dataset(df)
