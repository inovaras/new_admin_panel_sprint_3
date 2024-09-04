from concurrent.futures import ThreadPoolExecutor
from etl import *

def main():
    with ThreadPoolExecutor(max_workers=3) as pool:  # Устанавливаем max_workers на 3 для выполнения всех задач параллельно
        tasks = [etl_filmwork, etl_genres, etl_persons]
        futures = [pool.submit(task) for task in tasks]

        for future in futures:
            try:
                future.result()
            except Exception as e:
                logger.error(f"Ошибка при выполнении задачи: {str(e)}")

    logger.info("Все ETL процессы завершены.")

if __name__ == "__main__":
    main()


# def thread_receiving():
#     while True:
#         message = my_socket.recv(1024).decode()
#         if not message:
#             break
#         print(message)

# create ThreadPool(workers=3)
# if __name__ == "__main__":
#     with ThreadPoolExecutor(max_workers=2) as pool:
#         tasks = [thread_sending, thread_receiving]
#         futures = [pool.submit(task) for task in tasks]
#     logger.info("END")
