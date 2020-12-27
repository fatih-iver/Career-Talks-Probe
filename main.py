import datetime
import os
import threading
from threading import Thread

import requests
import sendgrid as sendgrid
from googleapiclient.discovery import build
from sendgrid import Email, To, Content, Mail

receiving_service_url = os.getenv("ANCHOR_AUTOMATOR_ADDRESS")
lock = threading.Lock()
date_of_x_days_ago = datetime.date.today() - datetime.timedelta(days=int(os.getenv("PROBE_FOR_X_DAYS_AGO")))

candidate_video_ids = []

career_talks_video_ids = []

failed_video_ids = []

video_id_to_title = {}


def trigger_anchor_automator(video_id):
    global failed_video_ids
    print(threading.get_ident(), "automation started for", str(video_id))
    try:
        metadata_server_token_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts" \
                                    "/default/identity?audience="
        token_request_url = metadata_server_token_url + receiving_service_url
        token_request_headers = {'Metadata-Flavor': 'Google'}
        token_response = requests.get(token_request_url, headers=token_request_headers)
        jwt = token_response.content.decode("utf-8")
        receiving_service_headers = {"Authorization": f"Bearer {jwt}"}
        service_response = requests.get(receiving_service_url, headers=receiving_service_headers,
                                        params={"videoId": video_id})
        if service_response.status_code != 200:
            print(threading.get_ident(), "request failed for", str(video_id), service_response.status_code,
                  service_response.content)
            with lock:
                failed_video_ids.append(video_id)
    except Exception as e:
        print(threading.get_ident(), "exception occurred for", str(video_id))
        print(threading.get_ident(), repr(e))
        with lock:
            failed_video_ids.append(video_id)
    print(threading.get_ident(), "automation finished for", str(video_id))


def probe():
    youtube = build(
        "youtube",
        "v3",
        developerKey=os.getenv("YOUTUBE_API_KEY"))

    request = youtube.search().list(
        part="snippet",
        channelId="UCorsRJLRwp6XO4ePGydk3yQ",
        maxResults=50,
        publishedAfter=str(date_of_x_days_ago) + "T00:00:00+03:00",
        publishedBefore=str(date_of_x_days_ago) + "T23:59:59+03:00",
        type="video"
    )

    response = request.execute()

    print("youtube response", response)

    global candidate_video_ids

    global career_talks_video_ids

    global video_id_to_title

    candidate_videos = response["items"]

    for candidate_video in candidate_videos:

        video_id = candidate_video["id"]["videoId"]
        candidate_video_ids.append(video_id)

        title = candidate_video["snippet"]["title"]
        video_id_to_title[video_id] = title

        description = candidate_video["snippet"]["description"]
        if description.lstrip().startswith("Kariyer Sohbetleri"):
            career_talks_video_ids.append(video_id)

    print("candidate video ids", str(candidate_video_ids))
    print("candidate video ids & titles", str(video_id_to_title))
    print("career talks video ids", str(career_talks_video_ids))

    threads = []

    for podcast_video_id in career_talks_video_ids:
        threads.append(Thread(target=trigger_anchor_automator, args=[podcast_video_id]))

    for thread in threads:
        thread.start()

    print("start: wait threads")
    for thread in threads:
        thread.join()
    print("end: wait threads")


def send_mail(subject, content):
    from_email = Email("podcasts@cpaths.org")
    to_email = To("podcasts@cpaths.org")
    mail = Mail(from_email, to_email, subject, content)
    try:
        sg = sendgrid.SendGridAPIClient()
        sg.send(mail)
    except Exception as e:
        print("exception occurred while sending mail", repr(e))


def send_start_mail():
    print("begin: send start mail")
    subject = "Career Talks Probe Job Started For " + str(date_of_x_days_ago)
    send_mail(subject, Content("text/plain", "No-Content"))
    print("end: send finish mail")


def send_finish_mail():
    print("begin: send finish mail")

    subject = "Career Talks Probe Job Finished For " + str(date_of_x_days_ago)

    mail_content_list = list()

    mail_content_list.append("SUCCESS:")
    mail_content_list.append("\n")
    mail_content_list.append("\n")

    for video_id in video_id_to_title:
        if video_id in career_talks_video_ids and video_id not in failed_video_ids:
            title = video_id_to_title[video_id]
            mail_content_list.append(" - " + title + ": https://www.youtube.com/watch?v=" + video_id)
            mail_content_list.append("\n\n")

    mail_content_list.append("FAILURE:")
    mail_content_list.append("\n")
    mail_content_list.append("\n")

    for video_id in video_id_to_title:
        if video_id in failed_video_ids:
            title = video_id_to_title[video_id]
            mail_content_list.append(" - " + title + ": https://www.youtube.com/watch?v=" + video_id)
            mail_content_list.append("\n\n")

    mail_content_list.append("IGNORED:")
    mail_content_list.append("\n")
    mail_content_list.append("\n")

    for video_id in candidate_video_ids:
        if video_id not in career_talks_video_ids:
            title = video_id_to_title[video_id]
            mail_content_list.append(" - " + title + ": https://www.youtube.com/watch?v=" + video_id)
            mail_content_list.append("\n\n")

    content = Content("text/plain", "".join(mail_content_list))
    send_mail(subject, content)
    print("end: send finish mail")


def send_fail_mail(exception):
    print("begin: send fail mail")
    subject = "Career Talks Probe Job Failed For " + str(date_of_x_days_ago)
    content = Content("text/plain", exception)
    send_mail(subject, content)
    print("end: send fail mail")


def probe_trigger(request):
    print("begin: probe triggered")
    try:
        send_start_mail()
        probe()
        send_finish_mail()
    except Exception as e:
        print("failed: probe triggered")
        print(repr(e))
        send_fail_mail(repr(e))
    print("end: probe triggered")
    return "", 200
