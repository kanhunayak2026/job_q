# Reliable Job Queue with Dead-Letter Recovery

A backend job queue system built using **Python, FastAPI, Apache Kafka, and MySQL**. It processes background jobs asynchronously, retries failed jobs, tracks job status, and supports dead-letter recovery.

## 1. Project Overview

This project demonstrates how to move time-consuming tasks out of the main API request cycle and process them asynchronously using Kafka workers.

For example, when a user registers:
1. FastAPI creates the user in MySQL.
2. A job record is created in the database.
3. The job is published to Kafka.
4. A background consumer receives the job.
5. The consumer sends a welcome email using Resend.
6. The job status is updated in MySQL.

The API does not need to wait for the email to be sent before returning the registration response.

## 2. Tech Stack

- **Python** – Backend programming
- **FastAPI** – REST API development
- **Apache Kafka** – Message broker
- **MySQL** – Persistent storage
- **SQLAlchemy** – ORM and database operations
- **Docker** – Running Kafka
- **Resend** – Email delivery
- **Pydantic** – Request validation
- **pwdlib** – Password hashing

## 3. Architecture

```text
                 Client
                   |
                   v
             FastAPI Server
                   |
                   v
             MySQL Database
             /           \
       User Record     Job Record
                           |
                           v
                     Kafka Producer
                           |
                           v
                      Kafka Topic
                         jobs
                           |
                           v
                     Kafka Consumer
                           |
                           v
                     Process Job
                           |
                    Send Welcome Email
                           |
                  +--------+--------+
                  |                 |
                Success           Failure
                  |                 |
                  v                 v
              Completed        Retry Attempts
                                    |
                              Max Attempts
                                    |
                                    v
                               DLQ Topic
                               jobs.dlq
                                    |
                                    v
                              Dead Job
                                    |
                                    v
                             Manual Replay
```

## 4. Features

### Asynchronous Job Processing
- Publishes background jobs to Kafka.
- Processes jobs independently from API requests.
- Uses a Kafka consumer group named `job_workers`.

### Retry Mechanism
- Retries failed jobs up to 3 attempts.
- Uses exponential backoff between retries.
- Delay increases after each failed attempt.

### Dead Letter Queue (DLQ)
- Sends jobs to the `jobs.dlq` Kafka topic after all retries fail.
- Updates the corresponding database job status to `dead`.
- Stores error information for debugging.

### Job Status Tracking

Jobs are tracked in MySQL using the following statuses:

| Status | Description |
|---|---|
| pending | Waiting for processing |
| processing | Currently claimed by a worker |
| completed | Successfully processed |
| dead | Failed after maximum retries |

### Manual Offset Commit
- Kafka automatic offset commits are disabled.
- The consumer commits offsets after successful processing or after handling a job as dead.
- This helps prevent acknowledging a message before its processing outcome is recorded.

### Lease-Based Job Recovery
- Uses `locked_by` and `lease_until` fields in MySQL.
- Workers claim jobs using an atomic database update.
- If a worker crashes, another worker can claim the job after its lease expires.
- The current lease duration is 30 seconds.

### Dead Job Replay
- Provides an API endpoint to replay dead jobs.
- Resets the job status to `pending`.
- Resets the retry counter and republishes the job to Kafka.

### Password Hashing
- Hashes user passwords before storing them in MySQL.

## 5. API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| POST | `/register` | Registers a user and publishes a welcome-email job |
| GET | `/dlq` | Retrieves jobs with dead status |
| POST | `/dlq/replay/{job_id}` | Replays a dead job |

### Example: Register User

**Request**

```http
POST /register
Content-Type: application/json
```

```json
{
  "username": "kanhu",
  "email": "user@example.com",
  "password": "your-password"
}
```

The API creates the user and a background email job.

## 6. Retry and Failure Handling

When a job fails, the consumer retries it up to three times.

The retry delay follows exponential backoff:

| Attempt | Delay |
|---|---|
| 1 | Immediate |
| 2 | 1 second |
| 3 | 2 seconds |

After the final failed attempt:
1. The job is published to `jobs.dlq`.
2. Its MySQL status is updated to `dead`.
3. The error is stored in `last_error`.
4. The Kafka offset is committed.

## 7. Crash Recovery

The project implements lease-based job claiming.

Each worker has a unique ID. When it claims a job, the database stores:
- `locked_by`: Worker identifier.
- `lease_until`: Lease expiration timestamp.

If a worker crashes while processing a job, its lease eventually expires. Another worker can then claim the job, provided the Kafka message is delivered again.

This mechanism was tested by stopping one worker during processing, allowing the lease to expire, and starting another worker to process the redelivered message.

## 8. Database Design

### Users Table

| Column | Description |
|---|---|
| id | Primary key |
| username | User name |
| email | Unique email |
| hashed_password | Hashed password |

### Jobs Table

| Column | Description |
|---|---|
| id | Primary key |
| job_id | Unique job UUID |
| task | Job type |
| status | Current job status |
| attempts | Processing attempts |
| payload | Job data in JSON |
| last_error | Last processing error |
| created_at | Creation timestamp |
| updated_at | Last update timestamp |
| locked_by | Worker identifier |
| lease_until | Lease expiration time |

## 9. Project Structure

```text
job_que/
│
├── app/
│   ├── main.py
│   ├── database.py
│   ├── model.py
│   ├── producer.py
│   ├── consumer.py
│   └── topic.py
│
├── .env
├── .gitignore
├── requirements.txt
└── README.md
```

## 10. Setup and Installation

### Prerequisites

- Python
- MySQL
- Docker Desktop
- Git

### Step 1: Clone the repository

```bash
git clone https://github.com/kanhunayak2026/job_q.git
cd job_q
```

### Step 2: Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```powershell
venv\Scripts\activate
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Configure environment variables

Create a `.env` file:

```env
DATABASE_URL=your_mysql_connection_string
RESEND_API_KEY=your_resend_api_key
```

Do not commit your `.env` file or expose your credentials.

### Step 5: Start Kafka

Start Kafka using your Docker Kafka setup and ensure the broker is accessible at:

```text
localhost:9092
```

### Step 6: Create the Kafka topic

```bash
python -m app.topic
```

The current `topic.py` creates the `jobs` topic. Ensure the `jobs.dlq` topic also exists before processing failed jobs.

### Step 7: Start the FastAPI server

```bash
uvicorn app.main:app --reload
```

### Step 8: Start the Kafka consumer

Open another terminal, activate the virtual environment, and run:

```bash
python -m app.consumer
```

The API documentation is available at:

```text
http://127.0.0.1:8000/docs
```

## 11. Current Limitations

This is a learning and demonstration project. Some production-level reliability features are not implemented yet.

- No heartbeat mechanism for extending worker leases.
- Long-running jobs may outlive their lease and potentially be claimed by another worker.
- Duplicate external side effects, such as sending an email twice, are possible if a worker crashes after the email is sent but before completion is recorded.
- Database updates and Kafka publishing are separate operations; a transactional outbox is not implemented.
- The project does not guarantee exactly-once processing.

## 12. Future Improvements

- Implement heartbeat-based lease renewal.
- Add idempotency to prevent duplicate external operations.
- Implement the transactional outbox pattern.
- Add structured logging and monitoring.
- Add Docker Compose for the complete application stack.
- Add more automated integration and failure-recovery tests.

## 13. Learning Outcomes

Through this project, I explored:

- Building REST APIs using FastAPI.
- Asynchronous processing using Kafka.
- Producer and consumer implementation.
- Consumer groups and manual offset commits.
- Retry mechanisms and exponential backoff.
- Dead-letter queue implementation.
- Database-backed job tracking.
- Atomic job claiming and lease-based recovery.
- Handling worker failures and message redelivery.

---

**Author:** Kanhu Nayak

**GitHub:** https://github.com/kanhunayak2026/job_q
