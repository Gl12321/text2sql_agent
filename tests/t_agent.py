import requests
from src.core.logger import setup_logger

logger = setup_logger("api_test")
URL = "http://127.0.0.1:8000/ask"


def test_api():
    test_queries_1 = [
        "Find the names of all students in the Comp. Sci. department",
        "Find the names and salaries of instructors who earn more than 70000",
        "Count the number of courses in each department"
    ]
    test_queries_2 = [
        "Find the titles of courses and the names of departments that offer them."
        "List the names of students and the names of their advisors."
        "Find the names of students who took courses in the 'Watson' building."
    ]

    for question in test_queries:
        logger.info(f"Sending question: {question}")
        try:
            response = requests.post(URL, json={"question": question}, timeout=360)

            if response.status_code == 200:
                data = response.json()
                logger.info(f"SQL: {data['sql']}")
                logger.info(f"Status: {data['status']}")
                if data['status'] == 'success':
                    logger.info(f"Data count: {len(data['data'])}")
                else:
                    logger.error(f"Execution error: {data['error']}")
            else:
                logger.error(f"Server error {response.status_code}: {response.text}")

        except Exception as e:
            logger.error(f"Connection error: {e}")


if __name__ == "__main__":
    test_api()
