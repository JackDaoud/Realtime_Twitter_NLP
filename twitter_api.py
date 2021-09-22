# Import the necessary methods from tweepy library
import json
import time
import boto3
import tweepy
import credentials


# This is a basic listener that just prints received tweets
class TweetStreamListener(tweepy.StreamListener):

    def on_data(self, data):
        try:
            # Get all the data from twitter
            all_data = json.loads(data)
            # Create a dictionary placeholder
            tw_data = {}

            # Pull data for extended tweets
            if 'extended_tweet' in all_data.keys():
                tw_data['created_at'] = str(all_data['created_at']),
                tw_data['id'] = str(all_data['id']),
                tw_data['user_name'] = str(all_data['user']['name']),
                tw_data['screen_name'] = str(all_data['user']['screen_name']),
                tw_data['follower_count'] = all_data['user']['followers_count'],
                tw_data['user_location'] = str(all_data['user']['location']),
                tw_data['extended_tweet'] = str(all_data['extended_tweet']['full_text']),
                tw_data['retweet_count'] = str(all_data['retweet_count'])

            # Pull data for normal length tweets
            elif 'text' in all_data.keys():
                tw_data['created_at'] = str(all_data['created_at']),
                tw_data['id'] = str(all_data['id']),
                tw_data['user_name'] = str(all_data['user']['name']),
                tw_data['screen_name'] = str(all_data['user']['screen_name']),
                tw_data['follower_count'] = all_data['user']['followers_count'],
                tw_data['user_location'] = str(all_data['user']['location']),
                tw_data['tweet'] = str(all_data['text']),
                tw_data['retweet_count'] = str(all_data['retweet_count'])

                # Convert values of the single tweet data into a list
                message = list(tw_data.items())

                # Print the data
                print('\n', message)

                # Stream
                try:
                    client.put_record(
                        DeliveryStreamName=delivery_stream,  # Name of Kinesis delivery stream
                        Record={
                            'Data': json.dumps(tw_data) + '\n'  # Data to be piped through the stream into S3
                        }
                    )
                except (AttributeError, Exception) as stream_e:
                    print(stream_e)

        except BaseException as data_e:
            print("failed on data ", str(data_e))
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':

    # Twitter Credentials
    consumer_key = credentials.twitter['api_key']
    consumer_secret = credentials.twitter['api_key_secret']
    access_token = credentials.twitter['access_token']
    access_token_secret = credentials.twitter['access_token_secret']

    # Twitter authentication and connection to Twitter's Streaming API
    listener = TweetStreamListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Connection to the AWS Kinesis Delivery Stream Firehose
    client = boto3.client('firehose',
                          region_name=credentials.aws['region'],
                          aws_access_key_id=credentials.aws['access_key_id'],
                          aws_secret_access_key=credentials.aws['secret_access_key']
                          )

    # Name of AWS Kinesis delivery stream
    delivery_stream = credentials.aws['delivery_stream_name']

    # Stream the data
    while True:
        try:
            print('Twitter streaming...')
            stream = tweepy.Stream(auth, listener)
            stream.filter(track=['vaccine', 'vaccination'], languages=['en'], stall_warnings=True)
        except Exception as e:
            print(e)
            print('Disconnected...')
            time.sleep(5)
            continue
