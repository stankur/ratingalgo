import boto3
from boto3.dynamodb.conditions import Key

from mpmath import betainc

import time


def beta_ppf(q, a, b):
    if q < 0 or q > 1:
        raise ValueError("Probability value q must be between 0 and 1.")
    if a <= 0 or b <= 0:
        raise ValueError("Parameters a and b must be greater than 0.")

    tol = 1e-3

    def f(x):
        return betainc(a, b, 0, x, regularized=True) - q

    x0 = 0.0
    x1 = 0.5
    x2 = 1.0

    while abs(x2 - x0) > tol:
        if f(x0) * f(x1) < 0:
            x2 = x1
        else:
            x0 = x1
        x1 = (x0 + x2) / 2

    return x1


def cumsum(arr):
    return [sum(arr[: i + 1]) for i in range(len(arr))]


def array_split(arr, indices):
    arrays = []
    start = 0
    for i in indices:
        arrays.append(arr[start:i])
        start = i
    arrays.append(arr[start:])

    return arrays


tableName = "foodwar"
table = boto3.resource("dynamodb").Table(tableName)


def split_into_proprtions(arr, proportions):
    arrlen = len(arr)
    cumSizes = cumsum(proportions)

    maxCum = cumSizes[-1]

    splitIndices = map(
        lambda cumSize: round(float(cumSize * arrlen) / float(maxCum)), cumSizes
    )

    return array_split(arr, list(splitIndices)[:-1])

dicoveredBayesianRankValues = {}

def get_bayesian_rank_value(restoCodeTuple):
    if (restoCodeTuple[1][0], restoCodeTuple[1][1]) in dicoveredBayesianRankValues:
        return dicoveredBayesianRankValues[(restoCodeTuple[1][0], restoCodeTuple[1][1])]

    result = beta_ppf(0.05, restoCodeTuple[1][0] + 1, restoCodeTuple[1][1] + 1)
    dicoveredBayesianRankValues[(restoCodeTuple[1][0], restoCodeTuple[1][1])] = result

    return result



def get_tiers(restoCodeRating):
    restoCodeRatingTuples = list(restoCodeRating.items())


    sortedTuples = sorted(
        restoCodeRatingTuples, key=get_bayesian_rank_value, reverse=True
    )


    def get_restocode(restoCodeRatingTuple):
        return restoCodeRatingTuple[0]

    sortedRestoCodes = map(
        get_restocode,
        sortedTuples,
    )

    return dict(
        zip(
            ["S", "A", "B", "C", "D", "F"],
            split_into_proprtions(list(sortedRestoCodes), [5, 18, 25, 8, 5, 4]),
        )
    )


def get_counter_rating(rating: int):
    return 5 - rating


def get_everything():
    responseWithInitialRating = table.query(
        KeyConditionExpression=Key("type").eq("initialRating")
    )

    itemsWithInitialRating = responseWithInitialRating["Items"]

    restoCodeRating = {}

    for item in itemsWithInitialRating:
        itemInitialRating = item["rating"]
        itemInitialCounterRating = get_counter_rating(itemInitialRating)

        initialRatingMultiplier = 2

        restoCodeRating[item["restocode"]] = (
            initialRatingMultiplier * itemInitialRating,
            initialRatingMultiplier * itemInitialCounterRating,
        )

    responseWithRating = table.query(
        KeyConditionExpression=Key("type").eq("userRating")
    )

    itemsWithRating = responseWithRating["Items"]

    for item in itemsWithRating:
        currRestoCode = item["restocode"]

        prevRatingSum = restoCodeRating[currRestoCode][0]
        prevCounterRatingSum = restoCodeRating[currRestoCode][1]

        currRating = item["rating"]

        restoCodeRating[currRestoCode] = (
            prevRatingSum + currRating,
            prevCounterRatingSum + get_counter_rating(currRating),
        )

    return get_tiers(restoCodeRating)


def convertStringRatingToInt(rating: str) -> int:
    capitalized = rating.capitalize()

    if capitalized == "S":
        return 5
    if capitalized == "A":
        return 4
    if capitalized == "B":
        return 3
    if capitalized == "C":
        return 2
    if capitalized == "D":
        return 1
    return 0


# requires rating to be an integer
def post_handler(event, context):
    if (
        not str(event.user)
        or (event.rating not in ["S", "A", "B", "C", "D", "F"])
        or str(event.restocode)
    ):
        return get_handler()

    table.put_item(
        Item={
            "type": "userRating",
            "restocode": str(event.restocode),
            "rating": convertStringRatingToInt(event.rating),
            "time": time.time(),
            "user": str(event.user),
        }
    )

    return get_everything()


def get_handler(event, context):
    return get_everything()
