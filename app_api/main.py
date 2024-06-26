import uvicorn


def main():
    while True:
        try:
            uvicorn.run(
                "app:app",
                host='127.0.0.1',
                port=8080,
                reload=True
            )
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main()
