import logging
import json
import io

import pandas as pd

import port.api.props as props
from port.api.commands import (CommandSystemDonate, CommandUIRender)

from ddpinspect import unzipddp
from ddpinspect import twitter
from ddpinspect import instagram
from ddpinspect import youtube
from ddpinspect import facebook
from ddpinspect.validate import Language


LOG_STREAM = io.StringIO()

logging.basicConfig(
    stream=LOG_STREAM,
    level=logging.INFO,
    format="%(asctime)s --- %(name)s --- %(levelname)s --- %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

LOGGER = logging.getLogger(__name__)

TABLE_TITLES = {
    "twitter_interests": props.Translatable(
        {
            "en": "Your interests according to Twitter:",
            "nl": "Jouw interesses volgens Twitter:",
        }
    ),
    "twitter_account_created_at": props.Translatable(
        {
            "en": "Creation date of your account on Twitter:",
            "nl": "Datum waarop je account is aangemaakt op Twitter:",
        }
    ),
    "instagram_your_topics": props.Translatable(
        {
            "en": "Topics your interested in according to Instagram:",
            "nl": "Onderwerpen waar jij volgens Instagram geintereseerd in bent:",
        }
    ),
    "instagram_interests": props.Translatable(
        {
            "en": "Your interests according to Instagram:",
            "nl": "Jouw interesses volgens Instagram:",
        }
    ),
    "instagram_account_created_at": props.Translatable(
        {
            "en": "Creation date of your account on Instagram:",
            "nl": "Datum waarop je account is aangemaakt op Instagram:",
        }
    ),
    "facebook_your_topics": props.Translatable(
        {
            "en": "Topics your interested in according to Facebook:",
            "nl": "Onderwerpen waar jij volgens Facebook geintereseerd in bent:",
        }
    ),
    "facebook_interests": props.Translatable(
        {
            "en": "Your interests according to Facebook:",
            "nl": "Jouw interesses volgens Facebook:",
        }
    ),
    "facebook_account_created_at": props.Translatable(
        {
            "en": "Creation date of your account on Facebook:",
            "nl": "Datum waarop je account is aangemaakt op Facebook:",
        }
    ),
    "youtube_watch_history": props.Translatable(
        {
            "en": "Video's you watched on YouTube:",
            "nl": "Video's die je gekeken hebt op Youtube:",
        }
    ),
    "youtube_subscriptions": props.Translatable(
        {
            "en": "Channels you are subscribed to on Youtube:",
            "nl": "Kanalen waarop je geabboneerd bent op Youtube:",
        }
    ),
    "youtube_comments": props.Translatable(
        {
            "en": "Comments you posted on Youtube:",
            "nl": "Reacties die je hebt geplaats op Youtube:",
        }
    ),
    "empty_result_set": props.Translatable(
        {
            "en": "We could not extract any data:",
            "nl": "We konden de gegevens niet in je donatie vinden:",
        }
    ),
}


def process(sessionId):
    LOGGER.info("Starting the donation flow")
    yield donate_logs(f"{sessionId}-tracking")

    platforms = [
        ("Twitter", extract_twitter),
        ("Instagram", extract_instagram),
        ("Facebook", extract_facebook),
        ("YouTube", extract_youtube),
    ]

    # progress in %
    subflows = len(platforms)
    steps = 2
    step_percentage = (100 / subflows) / steps
    progress = 0

    for index, platform in enumerate(platforms):
        platform_name, extraction_fun = platform
        data = None

        # STEP 1: select the file
        progress += step_percentage
        while True:
            LOGGER.info("Prompt for file for %s", platform_name)
            yield donate_logs(f"{sessionId}-tracking")

            promptFile = prompt_file(platform_name, "application/zip, text/plain")
            fileResult = yield render_donation_page(platform_name, promptFile, progress)

            if fileResult.__type__ == "PayloadString":
                validation, extractionResult = extraction_fun(fileResult.value)

                # Flow: Three paths
                # 1: Extracted result: continue
                # 2: No extracted result: valid package, generated empty df: continue
                # 3: No extracted result: not a valid package, retry loop

                if extractionResult:
                    LOGGER.info("Payload for %s", platform_name)
                    yield donate_logs(f"{sessionId}-tracking")
                    data = extractionResult
                    break
                elif (
                    validation.status_code.id == 0
                    and not extractionResult
                    and validation.ddp_category is not None
                ):
                    LOGGER.info("Valid zip for %s; No payload", platform_name)
                    yield donate_logs(f"{sessionId}-tracking")
                    data = return_empty_result_set()
                    break
                elif validation.ddp_category is None:
                    LOGGER.info("Not a valid %s zip; No payload; prompt retry_confirmation", platform_name)
                    yield donate_logs(f"{sessionId}-tracking")
                    retry_result = yield render_donation_page(platform_name, retry_confirmation(platform_name), progress)

                    if retry_result.__type__ == "PayloadTrue":
                        continue
                    else:
                        LOGGER.info("Skipped during retry %s", platform_name)
                        yield donate_logs(f"{sessionId}-tracking")
                        data = return_empty_result_set()
                        break
            else:
                LOGGER.info("Skipped %s", platform_name)
                yield donate_logs(f"{sessionId}-tracking")
                break

        # STEP 2: ask for consent
        progress += step_percentage

        if data is not None:
            LOGGER.info("Prompt consent; %s", platform_name)
            yield donate_logs(f"{sessionId}-tracking")
            prompt = prompt_consent(platform_name, data)
            consent_result = yield render_donation_page(platform_name, prompt, progress)

            if consent_result.__type__ == "PayloadJSON":
                LOGGER.info("Data donated; %s", platform_name)
                yield donate_logs(f"{sessionId}-tracking")
                yield donate(platform_name, consent_result.value)
            else:
                LOGGER.info("Skipped ater reviewing consent: %s", platform_name)
                yield donate_logs(f"{sessionId}-tracking")

    yield render_end_page()


##################################################################
# helper functions

def prompt_consent(platform_name, data):
    table_list = []

    for k, v in data.items():
        df = v["data"]
        table = props.PropsUIPromptConsentFormTable(f"{platform_name}_{k}", v["title"], df)
        table_list.append(table)

    return props.PropsUIPromptConsentForm(table_list, [])


def return_empty_result_set():
    result = {}

    df = pd.DataFrame(["No data found"], columns=["No data found"])
    result["empty"] = {"data": df, "title": TABLE_TITLES["empty_result_set"]}

    return result


def donate_logs(key):
    log_string = LOG_STREAM.getvalue()  # read the log stream

    if log_string:
        log_data = log_string.split("\\n")
    else:
        log_data = ["no logs"]

    return donate(key, json.dumps(log_data))


##################################################################
# Extraction functions

def extract_twitter(twitter_zip):
    result = {}

    validation = twitter.validate_zip(twitter_zip)

    interests_bytes = unzipddp.extract_file_from_zip(twitter_zip, "personalization.js")
    interests_listdict = twitter.bytesio_to_listdict(interests_bytes)
    interests = twitter.interests_to_list(interests_listdict)

    if interests:
        df = pd.DataFrame(interests, columns=["Interests"])
        result["interests"] = {"data": df, "title": TABLE_TITLES["twitter_interests"]}
 
    account_created_at_bytes = unzipddp.extract_file_from_zip(twitter_zip, "account.js")  
    account_created_at_listdict = twitter.bytesio_to_listdict(account_created_at_bytes)
    account_created_at = twitter.account_created_at_to_list(account_created_at_listdict)

    if account_created_at:
        df = pd.DataFrame(account_created_at, columns=["Account created at"])
        result["account_created_at"] = {"data": df, "title": TABLE_TITLES["twitter_account_created_at"]}

    return validation, result


def extract_instagram(instagram_zip):
    result = {}

    validation = instagram.validate_zip(instagram_zip)

    interests_bytes = unzipddp.extract_file_from_zip(instagram_zip, "ads_interests.json")
    interests_dict = unzipddp.read_json_from_bytes(interests_bytes)
    interests = instagram.interests_to_list(interests_dict)
    if interests:
        df = pd.DataFrame(interests, columns=["Interests"])
        result["interests"] = {"data": df, "title": TABLE_TITLES["instagram_interests"]}

    your_topics_bytes = unzipddp.extract_file_from_zip(instagram_zip, "your_topics.json")
    your_topics_dict = unzipddp.read_json_from_bytes(your_topics_bytes)
    your_topics = instagram.your_topics_to_list(your_topics_dict)
    if your_topics:
        df = pd.DataFrame(your_topics, columns=["Your Topics"])
        result["your_topics"] = {"data": df, "title": TABLE_TITLES["instagram_your_topics"]}
  
    account_created_at_bytes = unzipddp.extract_file_from_zip(instagram_zip, "signup_information.json")
    account_created_at_dict = unzipddp.read_json_from_bytes(account_created_at_bytes)
    account_created_at = instagram.account_created_at_to_list(account_created_at_dict)
    if account_created_at:
        df = pd.DataFrame(account_created_at, columns=["Account created at"])
        result["account_created_at"] = {"data": df, "title": TABLE_TITLES["instagram_account_created_at"]}

    return validation, result


def extract_facebook(facebook_zip):
    result = {}

    validation = facebook.validate_zip(facebook_zip)

    interests_bytes = unzipddp.extract_file_from_zip(facebook_zip, "ads_interests.json")
    interests_dict = unzipddp.read_json_from_bytes(interests_bytes)
    interests = facebook.interests_to_list(interests_dict)
    if interests:
        df = pd.DataFrame(interests, columns=["Interests"])
        result["interests"] = {"data": df, "title": TABLE_TITLES["facebook_interests"]}

    your_topics_bytes = unzipddp.extract_file_from_zip(facebook_zip, "your_topics.json")
    your_topics_dict = unzipddp.read_json_from_bytes(your_topics_bytes)
    your_topics = facebook.your_topics_to_list(your_topics_dict)
    if your_topics:
        df = pd.DataFrame(your_topics, columns=["Your Topics"])
        result["your_topics"] = {"data": df, "title": TABLE_TITLES["facebook_your_topics"]}

    account_created_at_bytes = unzipddp.extract_file_from_zip(facebook_zip, "profile_information.json")
    account_created_at_dict = unzipddp.read_json_from_bytes(account_created_at_bytes)
    account_created_at = facebook.account_created_at_to_list(account_created_at_dict)
    if account_created_at:
        df = pd.DataFrame(account_created_at, columns=["Account created at"])
        result["account_created_at"] = {"data": df, "title": TABLE_TITLES["facebook_account_created_at"]}

    return validation, result


def extract_youtube(youtube_zip):
    result = {}

    validation = youtube.validate_zip(youtube_zip)
    if validation.ddp_category is not None:

        if validation.ddp_category.language == Language.EN:
            subscriptions_fn = "subscriptions.csv"
            watch_history_fn = "watch-history.json"
            comments_fn = "my-comments.html"
        else:
            subscriptions_fn = "abonnementen.csv"
            watch_history_fn = "kijkgeschiedenis.json"
            comments_fn = "mijn-reacties.html"

        subscriptions_bytes = unzipddp.extract_file_from_zip( youtube_zip, subscriptions_fn)
        subscriptions_listdict = unzipddp.read_csv_from_bytes(subscriptions_bytes)
        df = youtube.to_df(subscriptions_listdict)
        if not df.empty:
            result["subscriptions"] = {"data": df, "title": TABLE_TITLES["youtube_subscriptions"]}

        watch_history_bytes = unzipddp.extract_file_from_zip(youtube_zip, watch_history_fn)
        watch_history_listdict = unzipddp.read_json_from_bytes(watch_history_bytes)
        df = youtube.to_df(watch_history_listdict)
        if not df.empty:
            result["watch_history"] = {"data": df, "title": TABLE_TITLES["youtube_watch_history"]}

        comments_bytes = unzipddp.extract_file_from_zip(youtube_zip, comments_fn)
        df = youtube.comments_to_df(comments_bytes)
        if not df.empty:
            result["comments"] = { "data": df, "title": TABLE_TITLES["youtube_comments"]}

    return validation, result


##########################################
# Functions provided by Eyra did not change

def render_end_page():
    page = props.PropsUIPageEnd()
    return CommandUIRender(page)


def render_donation_page(platform, body, progress):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))

    footer = props.PropsUIFooter(progress)
    page = props.PropsUIPageDonation(platform, header, body, footer)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": f"Unfortunately, we cannot process your {platform} file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
            "nl": f"Helaas, kunnen we uw {platform} bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen."
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(platform, extensions):
    description = props.Translatable(
        {
            "en": "Please select this file so we can extract relevant information for our research.",
            "nl": "Je kan deze file nu selecteren zodat wij er relevante informatie uit kunnen halen voor ons onderzoek.",
        }
    )
    return props.PropsUIPromptFileInput(description, extensions)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)
