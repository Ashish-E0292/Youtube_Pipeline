import os
import time
import json
import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configuration
API_KEY = "AIzaSyBEDGArnsn19HcOZ0HoUHDHgy45iTOFyck"  # Your API key
INPUT_FILE = "Rechecked Channels List.xlsx"  # Your input file with @handles
OUTPUT_FILE = "youtube_raw_new_channels4.csv"   # Output as CSV
CHECKPOINT_FILE = "youtube_checkpoint_new.json"
BATCH_SIZE = 5  # Process this many channels before saving checkpoint

def setup_youtube_api():
    """Set up the YouTube API client."""
    return build('youtube', 'v3', developerKey=API_KEY)

def get_channel_id_from_handle(youtube, handle):
    """Convert @handle to a channel ID."""
    try:
        handle_clean = handle.strip()
        if handle_clean.startswith('@'):
            handle_clean = handle_clean[1:]
        request = youtube.search().list(
            part="snippet",
            q=f"@{handle_clean}",
            type="channel",
            maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
        request = youtube.search().list(
            part="snippet",
            q=handle_clean,
            type="channel",
            maxResults=1
        )
        response = request.execute()
        if response.get('items'):
            return response['items'][0]['snippet']['channelId']
        print(f"Could not find channel ID for handle: {handle}")
        return None
    except Exception as e:
        print(f"Error finding channel from handle {handle}: {e}")
        return None

def get_channel_info(youtube, channel_id):
    """Get detailed channel information."""
    try:
        request = youtube.channels().list(
            part="snippet,statistics,contentDetails",
            id=channel_id
        )
        response = request.execute()
        if not response.get('items'):
            return None
        channel = response['items'][0]
        snippet = channel.get('snippet', {})
        statistics = channel.get('statistics', {})
        uploads_playlist_id = channel.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads')
        return {
            'channel_id': channel_id,
            'channel': snippet.get('title', ''),
            'channel_user': snippet.get('customUrl', ''),
            'channel_description': snippet.get('description', ''),
            'subscriber_count': statistics.get('subscriberCount', 0),
            'video_count': statistics.get('videoCount', 0),
            'view_count': statistics.get('viewCount', 0),
            'uploads_playlist_id': uploads_playlist_id
        }
    except Exception as e:
        print(f"Error getting channel info: {e}")
        return None

def get_channel_videos(youtube, uploads_playlist_id):
    """Get ALL videos from a channel's uploads playlist without limitation."""
    videos = []
    next_page_token = None
    total_fetched = 0
    try:
        while True:
            request = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=uploads_playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            items_count = len(response.get('items', []))
            total_fetched += items_count
            for item in response.get('items', []):
                video_id = item['contentDetails']['videoId']
                videos.append({
                    'video_id': video_id,
                    'title': item['snippet']['title'],
                    'description': item['snippet']['description'],
                    'published': item['snippet']['publishedAt'],
                    'video_url': f"https://www.youtube.com/watch?v={video_id}"
                })
            print(f"  Fetched {total_fetched} videos so far...")
            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
            time.sleep(0.5)
    except Exception as e:
        print(f"Error getting videos: {e}")
    print(f"  Total videos fetched: {len(videos)}")
    return videos

def get_video_statistics(youtube, video_ids):
    """Get statistics for a list of videos."""
    results = []
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            request = youtube.videos().list(
                part="statistics,snippet,contentDetails",
                id=",".join(batch)
            )
            response = request.execute()
            for item in response.get('items', []):
                stats = item.get('statistics', {})
                results.append({
                    'video_id': item['id'],
                    'likes': stats.get('likeCount', 0),
                    'comments': stats.get('commentCount', 0),
                    'views': stats.get('viewCount', 0),
                    'tags': ','.join(item.get('snippet', {}).get('tags', []))
                })
        except Exception as e:
            print(f"Error getting video statistics: {e}")
        time.sleep(1)
    return results

def load_checkpoint():
    """Load the checkpoint file if it exists."""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {"processed_rows": 0, "results": []}

def save_checkpoint(processed_rows, results):
    """Save progress to the checkpoint file."""
    checkpoint = {
        "processed_rows": processed_rows,
        "results": results
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)
    print(f"Checkpoint saved. Processed {processed_rows} channels.")

def save_results_to_csv(results):
    """Save results to CSV file."""
    if results:
        df = pd.DataFrame(results)
        column_order = ['channel_user', 'video_id', 'title', 'description', 
                        'published', 'channel', 'channel_id', 'tags', 
                        'likes', 'comments', 'views', 'video_url']
        actual_columns = [col for col in column_order if col in df.columns]
        for col in df.columns:
            if col not in actual_columns:
                actual_columns.append(col)
        df = df[actual_columns]
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
        print(f"Data saved to {OUTPUT_FILE}")

def main():
    youtube = setup_youtube_api()
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    channel_column = 'channel_user'
    if channel_column not in df.columns:
        for col in df.columns:
            if df[col].astype(str).str.startswith('@').any():
                channel_column = col
                print(f"Using column '{channel_column}' for channel handles.")
                break
        else:
            print("Could not find a column with channel handles (@username).")
            return
    checkpoint = load_checkpoint()
    processed_rows = checkpoint["processed_rows"]
    results = checkpoint["results"]
    if processed_rows > 0:
        print(f"Resuming from row {processed_rows + 1}")
    try:
        for i in range(processed_rows, len(df)):
            handle = str(df.iloc[i][channel_column]).strip()
            print(f"Processing {i+1}/{len(df)}: {handle}")
            channel_id = get_channel_id_from_handle(youtube, handle)
            if not channel_id:
                continue
            channel_info = get_channel_info(youtube, channel_id)
            if not channel_info or not channel_info.get('uploads_playlist_id'):
                continue
            videos = get_channel_videos(youtube, channel_info['uploads_playlist_id'])
            if not videos:
                continue
            video_ids = [v['video_id'] for v in videos]
            video_stats = get_video_statistics(youtube, video_ids)
            video_stats_dict = {vs['video_id']: vs for vs in video_stats}
            for video in videos:
                video_result = {
                    'channel_user': handle,
                    'video_id': video['video_id'],
                    'title': video['title'],
                    'description': video['description'],
                    'published': video['published'],
                    'channel': channel_info['channel'],
                    'channel_id': channel_id,
                    'video_url': video['video_url']
                }
                stats = video_stats_dict.get(video['video_id'], {})
                video_result.update({
                    'tags': stats.get('tags', ''),
                    'likes': stats.get('likes', 0),
                    'comments': stats.get('comments', 0),
                    'views': stats.get('views', 0)
                })
                results.append(video_result)
            processed_rows = i + 1
            save_checkpoint(processed_rows, results)
            if processed_rows % BATCH_SIZE == 0 or i == len(df) - 1:
                save_results_to_csv(results)
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nProcess interrupted by user.")
        save_checkpoint(processed_rows, results)
        save_results_to_csv(results)
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        save_checkpoint(processed_rows, results)
        save_results_to_csv(results)
        return
    save_results_to_csv(results)
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("Checkpoint file removed after successful completion.")

if __name__ == "__main__":
    main()