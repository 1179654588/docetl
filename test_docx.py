def docx_to_string(
    input_path: str, 
    ocr_for_images: bool = False,
    lang: str = "en",
    lines_per_chunk: int = 100,  # 每块最大行数
    overlap_lines: int = 5       # 语义重叠行数
) -> list[str]:
    """
    Extract text from a Word document. Supports both .doc and .docx formats.
    Can optionally use OCR for embedded images. Supports both local files and URLs.

    Args:
        input_path (str): Path to the docx/doc file or URL.
        ocr_for_images (bool): If True, use PaddleOCR to extract text from images.
        lang (str): Language for OCR (default: "en").
        lines_per_chunk (int): Maximum number of lines per chunk.
        overlap_lines (int): Number of overlapping lines between chunks.

    Returns:
        list[str]: Extracted text from the document.
    """
    import sys
    import tempfile
    import requests
    from docx import Document
    from docx.oxml.text.paragraph import CT_P
    from docx.oxml.table import CT_Tbl
    from docx.parts.image import ImagePart
    from docx.text.paragraph import Paragraph
    import io, os
    
    def convert_doc_to_docx_stream(doc_stream: io.BytesIO) -> io.BytesIO:
        """Convert .doc stream to .docx stream"""
        if sys.platform == 'win32':
            try:
                from win32com import client as wc
                # 保存临时doc文件
                with tempfile.NamedTemporaryFile(delete=False, suffix='.doc') as tmp_doc:
                    tmp_doc.write(doc_stream.read())
                    tmp_doc_path = tmp_doc.name
                
                try:
                    word = wc.Dispatch("Word.Application")
                    doc = word.Documents.Open(tmp_doc_path)
                    
                    # 保存为docx
                    tmp_docx_path = tmp_doc_path + 'x'
                    doc.SaveAs(tmp_docx_path, 12)
                    doc.Close()
                    word.Quit()
                    
                    # 读取转换后的文件到流
                    with open(tmp_docx_path, 'rb') as f:
                        docx_stream = io.BytesIO(f.read())
                    
                    # 清理临时文件
                    os.remove(tmp_doc_path)
                    os.remove(tmp_docx_path)
                    
                    return docx_stream
                except Exception as e:
                    if os.path.exists(tmp_doc_path):
                        os.remove(tmp_doc_path)
                    raise ValueError(f"doc文件转换失败: {str(e)}")
            except ImportError:
                raise ValueError("Windows系统需要安装pywin32才能转换doc文件")
    
    def is_image_paragraph(p, doc):
        """判断段落是否是图片"""
        try:
            graph = Paragraph(p, doc)
            images = graph._element.xpath('.//pic:pic')
            for image in images:
                for img_id in image.xpath('.//a:blip/@r:embed'):
                    part = doc.part.related_parts[img_id]
                    if isinstance(part, ImagePart):
                        return True, part
        except:
            pass
        return False, None
    
    def extract_text_from_image(image_part, lang):
        """使用PaddleOCR从图片中提取文字"""
        import numpy as np
        from PIL import Image
        from paddleocr import PaddleOCR
        
        if lang == "cn":
            lang = "ch"
        ocr = PaddleOCR(use_angle_cls=True, lang=lang)
        
        # 将图片数据转换为numpy数组
        img_data = image_part.blob
        img = Image.open(io.BytesIO(img_data)).convert('RGB')
        img_array = np.array(img)
        
        # 执行OCR
        ocr_result = ocr.predict(img_array)
        
        texts = []
        if ocr_result and isinstance(ocr_result, list) and len(ocr_result) > 0:
            result_dict = ocr_result[0]  # 获取第一个(也是唯一一个)字典
            if result_dict.get('rec_texts'):
                # 只提取文本内容
                texts += "\n".join(result_dict['rec_texts']) + "\n"
        
        return "\n".join(texts)
    
    lines_per_chunk = int(lines_per_chunk)
    overlap_lines = int(overlap_lines)
    # 1. 处理网络路径 (URL)
    file_stream = None
    # 1. 获取文件流
    if input_path.startswith(("http://", "https://")):
        headers = {"User-Agent": "Mozilla/5.0..."}
        response = requests.get(input_path, headers=headers, timeout=30)
        file_stream = io.BytesIO(response.content)
        file_ext = os.path.splitext(input_path.split('?')[0])[1].lower()
    else:
        with open(input_path, 'rb') as f:
            file_stream = io.BytesIO(f.read())
        file_ext = os.path.splitext(input_path)[1].lower()

    if file_ext == '.doc':
        file_stream = convert_doc_to_docx_stream(file_stream)

    doc = Document(file_stream)

    # --- 核心逻辑：按行数进行语义分块 (方法3) ---
    all_lines = [] # 存储文档提取出的所有原始行

    for element in doc.element.body:
        if isinstance(element, CT_P):
            is_img, img_part = is_image_paragraph(element, doc)
            if is_img and ocr_for_images:
                try:
                    ocr_text = extract_text_from_image(img_part, lang)
                    if ocr_text:
                        all_lines.extend(ocr_text.split('\n'))
                except:
                    all_lines.append("[图片OCR提取失败]")
            else:
                # 即使是空行也保留，维持文档原貌
                all_lines.append(element.text if element.text else "")
        
        elif isinstance(element, CT_Tbl):
            # 表格处理：每行单元格合并为一行文本
            for row in element.tr_lst:
                row_text = "\t".join([p.text for cell in row.tc_lst for p in cell.p_lst if p.text])
                if row_text:
                    all_lines.append(row_text)

    # 3. 执行分块逻辑
    chunks = []
    start_idx = 0
    total_lines = len(all_lines)

    while start_idx < total_lines:
        # 计算当前块的结束位置
        end_idx = start_idx + lines_per_chunk
        chunk_content = all_lines[start_idx:end_idx]
        
        if chunk_content:
            chunks.append("\n".join(chunk_content))
        
        # 步进：移动 lines_per_chunk 减去 重叠行数
        # 比如从 0-100，下一次从 95-195
        start_idx += (lines_per_chunk - overlap_lines)
        
        # 防止死循环或无效步进
        if (lines_per_chunk - overlap_lines) <= 0:
            break

    return chunks if chunks else [""]
    

if __name__ == "__main__":
    # Example usage
    input_pdf = "https://gxt.jiangxi.gov.cn/doc/ucap/1749973237782081536/document/20260106/FST3CSb2.doc"
    content = docx_to_string(input_pdf, ocr_for_images=True, lang="ch", lines_per_chunk=100, overlap_lines=5)
    print(content)