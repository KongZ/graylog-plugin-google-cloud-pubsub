# Overview
A Graylog's Google Cloud Pub/Sub plugin allows you to pull logs from [Google Cloud Stackdriver Logging](https://cloud.google.com/logging/) via Cloud Pub/Sub service.

# Setup Google Cloud Pub/Sub
Currently, this plugin supports only subscription delivery type *Pull*. To setup the log exporting on Google Cloud Stackdriver, do the following steps
1) Goto Cloud Pub/Sub menu. 
2) Create a new topic.
3) Create a new subscription and choose delivery type *Pull*

More detail https://cloud.google.com/logging/docs/export/

# Setup Graylog Input Plugin
1) Goto System -> Input menu.
2) Choose a *Google Cloud Pub/Sub Pull" from drop-down menu and click Launch new Input
3) Enter Project ID, Subscription Name and Credential file location e.g. `/etc/graylog/google-credential.json`. The credential file must be located on every Graylog's nodes at the same location. The service account for this credential requires `Pub/Sub Subscriber ` Role.

