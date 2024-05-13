import boto3
import os
from dotenv import load_dotenv
from typing import Union
import logging
from fastapi import FastAPI, Request, status, Header
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

from getSignedUrl import getSignedUrl

load_dotenv()

app = FastAPI()
logger = logging.getLogger("uvicorn")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
	exc_str = f'{exc}'.replace('\n', ' ').replace('   ', ' ')
	logger.error(f"{request}: {exc_str}")
	content = {'status_code': 10422, 'message': exc_str, 'data': None}
	return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)


class Post(BaseModel):
    title: str
    body: str


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.getenv("DYNAMO_TABLE"))

@app.post("/posts")
async def post_a_post(post: Post, authorization: str | None = Header(default=None)):

    logger.info(f"title : {post.title}")
    logger.info(f"body : {post.body}")
    logger.info(f"user : {authorization}")

    import uuid
    str_post_id = f'{uuid.uuid4()}'

    # Insert the post data into DynamoDB
    data = table.put_item(
        Item={
            'user': "USER#"+authorization,
            'id': "POST#"+str_post_id,
            'title': post.title,
            'body': post.body,
        }
    )
    return data 

@app.get("/posts")
async def get_all_posts(user: Union[str, None] = None):

    response = table.scan()
    if user: # Si un seul user
        posts = [item for item in response.get('Items', []) if item.get('user') == user]
    else: # Tous les posts
        posts = response.get('Items', [])

    return posts

@app.delete("/posts/{post_id}")
async def delete_post(post_id: str, authorization: str | None = Header(default=None)):

    data = table.delete_item(
        Key={'user': "USER#"+authorization,
             'id': "POST#"+post_id}
    )

    # Delete the image from BucketS3 (optionnel)
    return data


@app.get("/signedUrlPut")
async def get_signed_url_put(filename: str,filetype: str, postId: str, authorization: str | None = Header(default=None)):
    return getSignedUrl(filename, filetype, postId, authorization)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080, log_level="debug")
