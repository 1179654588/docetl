def html_to_string(
    input_path: str,
    ocr_for_images: bool = True,
    lang: str = "en",
    lines_per_chunk: int = 100,
    overlap_lines: int = 5
) -> list[str]:
    """
    Extract text from an HTML file or URL. Can optionally use OCR for embedded images.
    
    Args:
        input_path (str): Path to the HTML file or URL.
        ocr_for_images (bool): If True, use PaddleOCR to extract text from images.
        lang (str): Language for OCR (default: "en").
        lines_per_chunk (int): Maximum number of lines per chunk.
        overlap_lines (int): Number of overlapping lines between chunks.
    
    Returns:
        list[str]: Extracted text from the HTML document.
    """
    import requests
    import io
    from bs4 import BeautifulSoup
    import numpy as np
    from PIL import Image
    from paddleocr import PaddleOCR
    
    def extract_text_from_image_url(img_url: str, base_url: str, lang: str, ocr: PaddleOCR) -> str:
        """从图片URL中使用OCR提取文字"""
        try:
            # 处理相对路径
            if not img_url.startswith(("http://", "https://")):
                from urllib.parse import urljoin
                img_url = urljoin(base_url, img_url)
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "/".join(img_url.split("/")[:-1]) # 有些服务器会检查来源
            }
            response = requests.get(img_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            img = Image.open(io.BytesIO(response.content)).convert('RGB')
            img_array = np.array(img)
            
            ocr_result = ocr.predict(img_array)
            
            texts = []
            if ocr_result and isinstance(ocr_result, list) and len(ocr_result) > 0:
                result_dict = ocr_result[0]
                if result_dict.get('rec_texts'):
                    texts = result_dict['rec_texts']

            # 合并所有文本
            full_text = "\n".join(texts) if texts else ""
            
            # 检查是否包含中文字符
            if full_text:
                import re
                has_chinese = bool(re.search(r'[\u4e00-\u9fff]', full_text))
                if not has_chinese:
                    return ""  # 没有中文则返回空字符串
            
            return "\n".join(texts) if texts else "[图片OCR提取失败]"
        except Exception as e:
            print(f"图片OCR提取失败，URL: {img_url}，错误: {str(e)}")
            return f"[图片处理失败: {str(e)}]"
    
    lines_per_chunk = int(lines_per_chunk)
    overlap_lines = int(overlap_lines)
    # 1. 获取HTML内容
    if input_path.startswith(("http://", "https://")):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "/".join(input_path.split("/")[:-1])
        }
        response = requests.get(input_path, headers=headers, timeout=30)
        response.raise_for_status()
        response.encoding = response.apparent_encoding
        html_content = response.text
        base_url = input_path
    else:
        with open(input_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        base_url = ""
    
    # 2. 解析HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # 移除script和style标签
    for script in soup(["script", "style"]):
        script.decompose()
    
    all_lines = []
    
    ocr = PaddleOCR(use_angle_cls=True, lang=lang)

    # 3. 遍历HTML元素
    for element in soup.descendants:
        # 处理文本节点
        if element.name is None:  # NavigableString
            text = str(element).strip()
            if text:
                all_lines.append(text)
        
        # 处理图片标签
        elif element.name == 'img' and ocr_for_images:
            img_src = element.get('src')
            if img_src:
                ocr_text = extract_text_from_image_url(img_src, base_url, lang, ocr)
                if ocr_text:
                    all_lines.extend(ocr_text.split('\n'))
        
        # 处理表格
        elif element.name == 'tr':
            cells = element.find_all(['td', 'th'])
            if cells:
                row_text = "\t".join([cell.get_text(strip=True) for cell in cells])
                if row_text:
                    all_lines.append(row_text)
    
    # 4. 执行分块逻辑
    chunks = []
    start_idx = 0
    total_lines = len(all_lines)
    
    while start_idx < total_lines:
        end_idx = start_idx + lines_per_chunk
        chunk_content = all_lines[start_idx:end_idx]
        
        if chunk_content:
            chunks.append("\n".join(chunk_content))
        
        start_idx += (lines_per_chunk - overlap_lines)
        
        if (lines_per_chunk - overlap_lines) <= 0:
            break
    
    return chunks if chunks else [""]

if __name__ == "__main__":
    # 解析网络HTML
    result = html_to_string(
        input_path="https://gxt.hlj.gov.cn/gxt/c106958/202511/c00_31887853.shtml",
        ocr_for_images=True,
        lang="ch",
        lines_per_chunk=100,
        overlap_lines=5
    )
    print(result)