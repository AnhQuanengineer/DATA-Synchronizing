import json

# Danh sách chuỗi JSON đầu vào
input_data = [
    '{"user_id": 9, "login": "ivy333", "gravatar_id": "yz333", "avatar_url": "https://example.com/avatar9.png", "url": "https://example.com/users/ivy333", "state": "INSERT", "log_timestamp": "2025-08-22 02:13:19.779000"}',
    '{"user_id": 10, "login": "jack444", "gravatar_id": "aaa444", "avatar_url": "https://example.com/avatar10.png", "url": "https://example.com/users/jack444", "state": "INSERT", "log_timestamp": "2025-08-22 02:13:19.779000"}'
]

# Tạo danh sách dictionary từ dữ liệu đầu vào
parsed_data = [json.loads(item) for item in input_data]

print(parsed_data)

new = '{"user_id": 9, "login": "ivy333", "gravatar_id": "yz333", "avatar_url": "https://example.com/avatar9.png", "url": "https://example.com/users/ivy333", "state": "INSERT", "log_timestamp": "2025-08-22 02:13:19.779000"}'

print(json.loads(new))