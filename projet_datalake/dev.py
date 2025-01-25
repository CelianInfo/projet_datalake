# Original string with Unicode escape sequences
encoded_string = "Inconv\u00e9nients"

# Decode the string using the 'unicode-escape' codec
decoded_string = encoded_string.encode('utf-8').decode('unicode-escape')

print(decoded_string)