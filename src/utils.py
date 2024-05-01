

import datetime


def datetime_formater(iters):
    """
    The function `datetime_formater` iterates through a dictionary and converts any datetime values to a
    specific string format.
    
    :param iters: The `iters` parameter in the `datetime_formater` function seems to be a dictionary
    where the keys are strings and the values can be of any type. The function iterates over the keys of
    the dictionary and checks if the corresponding value is an instance of `datetime.datetime`. If it is
    :return: The function `datetime_formater` takes a dictionary `iters` as input, iterates over its
    keys, and checks if the corresponding value is an instance of `datetime.datetime`. If it is, the
    value is formatted as a string in the format "%Y-%m-%dT%H:%M:%S". The function then returns the
    modified dictionary `iters` with datetime values formatted as strings
    """
    for key in iters:
        if isinstance(iters[key], datetime.datetime):
            iters[key] = iters[key].strftime("%Y-%m-%dT%H:%M:%S")
    return iters