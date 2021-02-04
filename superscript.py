from lxutils.log import timer, exception


def main():
    with timer("Переношу отложенные с наступившим сроком возврата в квалификацию"):
        import postponed
    with timer("Назначаю ответственных по сделкам ответственными по контактам"):
        import assign_patch
    with timer("Сохраняю данные для отчетов Tableau"):
        import download
    with timer("Выгружаю отчет для АТИ"):
        import ati_report


try:
    exit(main())
except Exception:
    exception("Exception in main()")
    exit(1)
