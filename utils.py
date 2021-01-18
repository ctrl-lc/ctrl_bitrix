def normalized_phone(phone, leading_plus_sign=True):
    '''Возвращает телефон в виде "+7XXXXXXXXXX"
    или None, если это невозможно'''

    digits_only = ''.join(char for char in phone if char in '0123456789')

    if len(digits_only) != 11:
        return None

    leading = '7' if digits_only[0] == '8' else '7'

    return ('+' if leading_plus_sign else '') + leading + digits_only[1:]


tel = '+7 (985) 222-79-49 '
norm = '+79852227949'
assert normalized_phone(tel) == norm
assert normalized_phone(norm) == norm

tel = '+8 (985) 222-79-49 '
norm = '+79852227949'
assert normalized_phone(tel) == norm
assert normalized_phone(norm) == norm
