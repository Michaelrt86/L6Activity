# Music Streaming Analysis Using Spark Structured APIs

## Overview
Overall the idea of this Hands on Activity was for me to get comfortable with PySpark functions and how it essentially works. 

## Dataset Description
The Dataset is divided into two different portions

**listening_logs.csv**
This dataset is formatted like this<br>

user_id: String that says user_x where x represents number <br>
song_id: String that says song_x where x represents number <br>
timestamp: This is date data formatted as YYYY-MM-DD and does not include its own time within the timestamp <br>
duration_sec: Duration represents length listened to with format of HH-MM-SS-MS (Hours, Minutes, Seconds, Milliseconds)

**songs_metadata.csv**
This dataset is formatted like this<br>

song_id: same as attribute above
title: String attribute representing title
artist: String attribute representing artist
genre: String attribute representing genre
mood: String attribute representing mood


## Repository Structure
The repository structure consists of
datagen.py: Generates randomly generated data for both listening_logs and songs_metadata in CSV format

## Output Directory Structure
The output directory structure makes sure to output each tasks issue to its own unique folder that it then sends the CSV to in the proper format!

## Tasks and Outputs
I had done all of the tasks properly, I used Co-pilot at some points to help with minor details. I tried to make sure I understood what each component did in the code and was a little confused initially but over time I began to understand (I have been very busy so this helped me quite a lot). The Tasks completed include: <br>
**Task 1: User Favorite Genres**
This task was used to find each user's favorite genre and output it to the CSV with no duplicates! <br>
## Execution Instructions
## *Prerequisites*

Before starting the assignment, ensure you have the following software installed and properly configured on your machine:

1. *Python 3.x*:
   - [Download and Install Python](https://www.python.org/downloads/)
   - Verify installation:
     ```bash
     python3 --version
     ```

2. *PySpark*:
   - Install using pip:
     ```bash
     pip install pyspark
     ```

3. *Apache Spark*:
   - Ensure Spark is installed. You can download it from the [Apache Spark Downloads](https://spark.apache.org/downloads.html) page.
   - Verify installation by running:
     ```bash
     spark-submit --version
     ```

### *2. Running the Analysis Tasks*

####  *Running Locally*

1. *Generate the Input*:
  ```bash
   python3 input_generator.py
   ```

2. **Execute Each Task Using spark-submit**:
   ```bash
     spark-submit main.py
   ```

3. *Verify the Outputs*:
   Check the outputs/ directory for the resulting files:
   ```bash
   ls outputs/
   ```

## Errors and Resolutions
