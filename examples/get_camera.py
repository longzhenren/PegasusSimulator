import requests
import base64
from io import BytesIO
from PIL import Image

def fetch_and_save_image(uav_id, save_path):
    url = f"http://localhost:8080/uav/{uav_id}/all"

    try:
        # 发送 HTTP 请求并获取响应
        response = requests.get(url)
        
        # 如果请求失败，返回错误信息
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            return

        # 解析响应的 JSON 数据
        data = response.json()
        
        # 从 JSON 数据中提取 Base64 编码的图像数据
        image_data = data['image']['data']
        
        # 解码 Base64 图像数据
        img_data = base64.b64decode(image_data)
        
        # 将解码的图像数据加载为 PIL 图像
        image = Image.open(BytesIO(img_data))
        
        # 保存图像到本地
        image.save(f"{save_path}/{data['timestamp']}.png")
        
        print(f"Image saved to {save_path}/{data['timestamp']}.png")
    
    except Exception as e:
        print(f"Error: {e}")

# 示例调用
if __name__ == "__main__":
    # UAV ID
    uav_id = 0
    # 本地保存路径
    save_path = "output_image"
    
    # 调用函数获取并保存图像
    fetch_and_save_image(uav_id, save_path)