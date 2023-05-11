import boto3
import io
import functools
import pandas as pd
import asyncio


def cache_to_s3(prefix):
    def decorator(func):
        @functools.wraps(func)
        def rv(*args, **kwargs):
            if callable(prefix):
                s3_url = prefix(*args, **kwargs)
            else:
                s3_url = prefix
            try:
                return s3_read_csv(s3_url)
            except:
                value = func(*args, **kwargs)
                s3_write_csv(s3_url, value)
                return value

        return rv

    return decorator


def cache_to_s3_async(prefix):
    def decorator(func):
        @functools.wraps(func)
        async def rv(*args, **kwargs):
            if callable(prefix):
                s3_url = prefix(*args, **kwargs)
            else:
                s3_url = prefix
            try:
                return await s3_read_csv_async(s3_url)
            except:
                value = await func(*args, **kwargs)
                await s3_write_csv_async(s3_url, value)
                return value

        return rv

    return decorator


s3 = boto3.Session(
    aws_access_key_id="foo", aws_secret_access_key="bar", region_name="us-east-1"
).resource("s3", endpoint_url="http://localhost:4566", use_ssl=False)


def get_s3_obj(s3_path):
    if not s3_path.startswith("s3://"):
        raise RuntimeError(f"URL did not start with s3://: {s3_path}")
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket = path_parts.pop(0)
    key = "/".join(path_parts)
    return s3.Object(bucket, key)


def s3_read_csv(url):
    s3_obj = get_s3_obj(url)
    with s3_obj.get()["Body"] as f:
        return pd.read_csv(f)


async def s3_read_csv_async(url):
    return await asyncio.to_thread(s3_read_csv, url)


def s3_exists(url):
    s3_obj = get_s3_obj(url)
    try:
        s3_obj.load()
        return True
    except:
        return False


async def s3_exists_async(url):
    return await asyncio.to_thread(s3_exists, url)


def s3_write_csv(url, dataframe):
    s3_obj = get_s3_obj(url)
    s3_obj.put(Body=dataframe.to_csv())


async def s3_write_csv_async(url, dataframe):
    return await asyncio.to_thread(s3_write_csv, url, dataframe)


# no longer used!
# def s3_read_pickle(url):
#     s3_obj = get_s3_obj(url)
#     with s3_obj.get()["Body"] as f:
#         return pickle.load(f)


# async def s3_read_pickle_async(url):
#     return await asyncio.to_thread(s3_read_pickle, url)


# def s3_write_pickle(url, value):
#     s3_obj = get_s3_obj(url)
#     s3_obj.put(Body=pickle.dumps(value))


# async def s3_write_pickle_async(url, value):
#     return await asyncio.to_thread(s3_write_pickle, url, value)
