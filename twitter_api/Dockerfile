# use python3 as the base image for the db api"
FROM python:3.7.7-buster

# set the shared work directory for assignment1
WORKDIR /usr/src/assignment2

#copy the python imports
COPY requirements.txt .

# install the importsdfrom requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

#copy all local files to workspace
COPY . .

#expose the container port
EXPOSE 5000

#run the api
CMD ["python3", "twitter_api.py"]
