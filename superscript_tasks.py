from lxutils.log import timer, exception

def main():
    with timer('Проставляю задачи по сделкам в работе без активных задач'):
        import tasks

try:
    exit(main())
except Exception:
    exception("Exception in main()")
    exit(1)

