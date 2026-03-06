import json
from pathlib import Path
from typing import List, Dict
import pdfplumber
from docx import Document
from docx.text.paragraph import Paragraph
from docx.oxml.text.paragraph import CT_P
from docx.oxml.table import CT_Tbl
from docx.parts.image import ImagePart


def split_pdf(file_path: str) -> List[Dict[str, any]]:
    """
    切割PDF文件，按页提取文本（仅提取可选择的文本，不进行OCR）
    
    Args:
        file_path: PDF文件路径
        
    Returns:
        包含页号和文本的字典列表
    """
    result = []
    
    with pdfplumber.open(file_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            print(f"正在处理第 {page_num}/{len(pdf.pages)} 页...")
            
            # 仅提取文本，不进行OCR
            text = page.extract_text() or ""
            
            result.append({
                "页号": page_num,
                "文本": text.strip()
            })
    
    return result


def _is_image(p, doc):
        """判断段落是否是图片"""
        graph = Paragraph(p, doc)
        images = graph._element.xpath('.//pic:pic')  # 获取所有图片
        for image in images:
            for img_id in image.xpath('.//a:blip/@r:embed'):  # 获取图片id
                part = doc.part.related_parts[img_id]  # 根据图片id获取对应的图片
                if isinstance(part, ImagePart):
                    return True
        return False


def split_word(file_path: str) -> List[Dict[str, any]]:
    """
    切割Word/WPS文件，按页提取文本（仅提取文本，不识别图片）
    
    Args:
        file_path: Word/WPS文件路径
        
    Returns:
        包含页号和文本的字典列表
    """
    doc = Document(file_path)
    result = []
    
    content = []
    for p in doc.element.body:
        if isinstance(p, CT_P):
            if _is_image(p, doc):
                continue
            if p.text:
                content.append(p.text)
        elif isinstance(p, CT_Tbl):
            for row in p.tr_lst:
                c = []
                for cell in row.tc_lst:
                    for p in cell.p_lst:
                        if isinstance(p, CT_P) and p.text:
                            c.append(p.text)
                content.append('\t'.join(c))

    for i in range(0, len(content), 100):
        chunk_content = content[i:i + 100]
        result.append({
            "index": i // 100 + 1,
            "text": '\n'.join(chunk_content)
        })
    
    return result


def split_document(file_path: str, output_path: str = None) -> List[Dict[str, any]]:
    """
    统一接口：根据文件类型切割文档并提取文本（仅文本，不含OCR）
    
    Args:
        file_path: 文档文件路径
        output_path: 输出JSON文件路径（可选）
        
    Returns:
        包含页号和文本的字典列表
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")
    
    # 根据文件扩展名选择处理方法
    suffix = file_path.suffix.lower()
    
    if suffix == '.pdf':
        result = split_pdf(str(file_path))
    elif suffix in ['.docx', '.doc', '.wps']:
        result = split_word(str(file_path))
    else:
        raise ValueError(f"不支持的文件格式: {suffix}")
    
    # 如果指定了输出路径，保存为JSON文件
    if output_path:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
    
    return result


if __name__ == "__main__":
    # # 使用示例
    # import sys
    
    # if len(sys.argv) < 2:
    #     print("使用方法: python split_pdf.py <文件路径> [输出JSON路径]")
    #     print("示例: python split_pdf.py document.pdf output.json")
    #     sys.exit(1)
    
    # input_file = r"D:\code\doc-etl\docetl\raw_file" + sys.argv[1]
    # output_file = r"D:\code\doc-etl\docetl\dataset" + sys.argv[2] if len(sys.argv) > 2 else None


    input_file = r"D:\code\doc-etl\docetl\raw_file\P020240304674484947950.docx"
    output_file = r"D:\code\doc-etl\docetl\dataset\docx_output.json"
    
    if not output_file:
        # 默认输出文件名
        output_file = Path(input_file).stem + "_output.json"
    
    try:
        result = split_document(input_file, output_file)
        print(f"成功处理 {len(result)} 页内容")
        print(f"结果已保存到: {output_file}")
    except Exception as e:
        print(f"错误: {e}")
        # sys.exit(1)
