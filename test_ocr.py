def paddleocr_pdf_to_string(
    input_path: str,
    doc_per_page: bool = False,
    ocr_enabled: bool = True,
    lang: str = "ch",
) -> list[str]:
    """
    Extract text and image information from a PDF file using PaddleOCR for image-based PDFs.

    **Note: this is very slow!!**

    Args:
        input_path (str): Path to the input PDF file.
        doc_per_page (bool): If True, return a list of strings, one per page.
            If False, return a single string.
        ocr_enabled (bool): Whether to enable OCR for image-based PDFs.
        lang (str): Language of the PDF file.

    Returns:
        list[str]: Extracted content as a list of formatted strings.
    """
    import fitz
    import numpy as np
    import requests  # 新增：用于处理网络请求
    import io        # 新增：用于处理内存流
    from paddleocr import PaddleOCR
    import logging

    # 1. 处理网络路径 (URL)
    if input_path.startswith(("http://", "https://")):
        print(f"检测到 URL，正在下载文件: {input_path}")
        
        # 添加伪装头，防止 403 错误
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "/".join(input_path.split("/")[:-1]) # 有些服务器会检查来源
        }
        
        try:
            response = requests.get(input_path, headers=headers, timeout=30)
            response.raise_for_status() 
        except requests.exceptions.HTTPError as e:
            # 如果还是 403，打印更详细的信息
            print(f"下载失败，状态码: {response.status_code}。服务器拒绝了请求。")
            raise e
            
        file_data = io.BytesIO(response.content)
        pdf = fitz.open(stream=file_data, filetype="pdf")
    else:
        # 如果是本地路径，按原逻辑打开
        pdf = fitz.open(input_path)

    # 2. 初始化 OCR (lang="cn" 会报错，这里确保是 "ch")
    if lang == "cn": lang = "ch"
    ocr = PaddleOCR(use_angle_cls=True, lang=lang)

    pdf_content = []

    with pdf:
        for page_num in range(len(pdf)):
            page = pdf[page_num]
            text = page.get_text()
            images = []

            # 提取图像信息 (bbox)
            for img_index, img in enumerate(page.get_images(full=True)):
                try:
                    rect = page.get_image_bbox(img)
                    images.append(f"Image {img_index + 1}: bbox {rect}")
                except Exception:
                    continue

            page_content = f"Page {page_num + 1}:\n"
            page_content += f"Text:\n{text}\n"
            page_content += "Images:\n" + "\n".join(images) + "\n"

            # 3. 如果页面没有文字且开启了 OCR，则转为图片识别
            if not text.strip() and ocr_enabled:
                mat = fitz.Matrix(2, 2) # 提高分辨率提高识别率
                pix = page.get_pixmap(matrix=mat)
                
                # 将渲染的图片转为 numpy 数组供 Paddle 使用
                img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(
                    pix.height, pix.width, 3
                )

                ocr_result = ocr.predict(img)
                page_content += "OCR Results:\n"
                # ocr_result 是一个列表,包含一个字典
                if ocr_result and isinstance(ocr_result, list) and len(ocr_result) > 0:
                    result_dict = ocr_result[0]  # 获取第一个(也是唯一一个)字典
                    if result_dict.get('rec_texts'):
                        # 只提取文本内容
                        page_content += "\n".join(result_dict['rec_texts']) + "\n"

            pdf_content.append(page_content)

    if not doc_per_page:
        return ["\n\n".join(pdf_content)]

    return pdf_content


if __name__ == "__main__":
    # Example usage
    input_pdf = "https://gyxxh.tj.gov.cn/ZWGK4147/ZCWJ6355/wjwj/202212/W020230303523233911628.pdf"
    content = paddleocr_pdf_to_string(input_pdf, doc_per_page=True, ocr_enabled=True, lang="ch")
    print(content)