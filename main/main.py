import json
from flask import Flask, request, render_template
from twitter.def_twitter_data import get_tweets_data
import time
from aws_services.def_boto3_aws import upload_file_to_s3, \
    read_file_from_s3, connect_to_s3, \
    firehose_to_s3, \
    aws_upload_dir, \
    write_log_dynamodb

from news.def_get_news_data_request import get_news_api_request
from spark.def_spark_rt import create_spark_df
from google_services.def_google_services import sentimental_api, sentimental_api_score

# initiate vars that will use us further to get the user's input
get_user_input = {}
tweeter_data = ''
check_user_input = True

'''
flask start here
'''

# initiate flask
app = Flask(__name__, template_folder="../ui")


# define the homepage
@app.route('/')
def homepage():
    return render_template('ui_final_project.html')

# define the url to render for submitting.
@app.route('/ui_submit', methods=['POST', 'GET'])
def ui_submit():
    return render_template('ui_submit.html')

# define the route for getting the data
@app.route('/ui_submit_data', methods=['post', 'get'])
def ui_submit_data():
    data = request.json
    # convert the datetime to a key for the request_key
    specialChars = "-:T "
    request_key = data["From Date"]
    for specialChar in specialChars:
        request_key = request_key.replace(specialChar, '')

    # convert the keywords for the request_key
    keywords_for_req_key = data["keywords"].replace(',', '_')

    '''
    You can also use:
    keywords_for_req_key = keywords_for_req_key.replace(',', '_')
    keywords_for_req_key = keywords_for_req_key.replace(" ","")
    '''

    request_key = request_key + '_' + data["category"] + '_' + keywords_for_req_key
    print("request key: " + request_key)
    data["request_key"] = request_key
    response = json.dumps(data, indent=4)
    print(response)

    # upload the input data to s3 in the request bucket under a unique folder
    upload_file_to_s3(response, 'new-requests', request_key, request_key + '.json')
    get_user_input_aws = read_file_from_s3('new-requests',
                                           request_key,
                                           request_key + '.json')
    # get the request details
    get_user_input_aws_data = json.loads(get_user_input_aws)

    # start the get tweets process
    counter = 0
    while counter < 4:
        try:
            tweeter_data = get_tweets_data(get_user_input_aws_data["keywords"], 30)
            tweeter_data_json = json.loads(tweeter_data)
            tweeter_data_json["key_folder"] = request_key
            tweeter_data_json = json.dumps(tweeter_data_json, indent=4)
        except Exception as e:
            write_log_dynamodb(f"json from twitter failed: ", e)

        twitter_results_to_s3 = firehose_to_s3("Twitter-Data-Kinesis-Dynamic", tweeter_data_json)

        try:
            news_data = get_news_api_request(json.loads(get_user_input_aws), counter)
            news_data_json = json.loads(news_data)
            news_data_json["key_folder"] = request_key
            news_data_json = json.dumps(news_data_json, indent=4)
            news_results_to_s3 = firehose_to_s3("News-Data-Kinesis-Dynamic", news_data_json)
        except Exception as e:
            write_log_dynamodb("json data from news has failed: ", e)
        counter = + 1
        time.sleep(180)
        # convert twitter data into sparkDF
        twitter_spark_df = create_spark_df('twitter-data-api', request_key)

        # convert news data into sparkDF
        news_spark_df = create_spark_df('news-data-api', request_key)
    return response


# run flask with a debugging mode. The localhost is for tunneling
if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host="localhost", port=5000, debug=True)
'''
flask end here
'''
