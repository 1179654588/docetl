import os
import tempfile
import shutil
from typing import Optional, Dict, Any, List

# 默认系统环境配置
os.environ['ARK_API_KEY'] = 'afda58f0-0e4e-423d-904e-de22e0c6854b'
os.environ['OPENAI_API_BASE'] = 'https://ark.cn-beijing.volces.com/api/v3'
os.environ['OPENAI_API_KEY'] = 'afda58f0-0e4e-423d-904e-de22e0c6854b'
os.environ['MODEL_NAME'] = 'doubao-seed-1-8-251228'
os.environ['PYTHONUTF8'] = '1'

from docetl.api import Pipeline, Dataset, MapOp, PipelineStep, PipelineOutput, CodeReduceOp


# 不同文件类型的默认解析函数映射
DEFAULT_PARSING_FUNCTIONS = {
    "pdf": "paddleocr_pdf_to_string",
    "docx": "docx_to_string",
    "excel": "xlsx_to_string",
    "html": "html_to_string"
}

def extract_companies(
    input_data: List[Dict[str, str]],
    file_type: str,
    output_path: Optional[str] = None,
    intermediate_dir: Optional[str] = None,
    parsing_config: Optional[Dict[str, Any]] = None,
    dataset_type: str = "memory",
    dataset_source: str = "local",
    prompt: Optional[str] = None
) -> tuple:
    """从文档中提取企业信息
    
    Args:
        input_data: 输入文件路径或URL（如果是memory类型则为包含url的字典列表）
        file_type: 文件类型，支持 "pdf", "docx", "excel", "html"
        output_path: 输出文件路径，默认为 output/{file_type}_output.json
        intermediate_dir: 中间文件目录，默认为 output/intermediates
        parsing_config: 解析配置参数（手动传入所有参数，例如：
            {
                "ocr_enabled": True,
                "doc_per_page": True,
                "lang": "ch",
                "max_rows_per_str": 50
            }
        ）
        dataset_type: 数据集类型，"file" 或 "memory"
        dataset_source: 数据源类型，默认 "local"
        
    Returns:
        tuple: (cost, output) - 执行成本和输出结果
    """
    # 默认模型和系统提示
    default_model = "volcengine/doubao-seed-1-8-251228"
    system_prompt = {
        "dataset_description": "从文档解析出的段落文本，包含企业相关的表格信息。",
        "persona": "一个专业的文档数据提取专家，擅长从非结构化文本中精准还原表格数据。"
    }

    # 验证文件类型
    if file_type not in DEFAULT_PARSING_FUNCTIONS:
        raise ValueError(f"不支持的文件类型: {file_type}，支持的类型: {list(DEFAULT_PARSING_FUNCTIONS.keys())}")
    
    if output_path is None:
        output_path = os.path.join(tempfile.gettempdir(), f"{file_type}_output.json")
    if intermediate_dir is None:
        intermediate_dir = os.path.join(tempfile.gettempdir(), "docetl_intermediates")
    
    # 构建解析配置
    final_parsing_config = {
        "input_key": "url",
        "output_key": "text",
        "function": DEFAULT_PARSING_FUNCTIONS[file_type]
    }
    
    # 如果用户提供了配置，合并到最终配置中
    if parsing_config:
        final_parsing_config.update(parsing_config)
    
    # 创建数据集
    dataset = Dataset(
        type=dataset_type,
        path=input_data,
        source=dataset_source,
        parsing=[final_parsing_config]
    )
    
    # 定义操作列表
    operations = []
    operation_names = []
    
    # 1. Map操作：提取企业信息
    extract_op = MapOp(
        name="extract_company_info",
        type="map",
        prompt=prompt or f"""
        请分析以下从 {file_type.upper()} 中提取的段落文本：
        {{{{ input.text }}}}
        
        你的任务是识别并提取文本中隐含的表格数据。请提取出所有提及的企业信息，并以列表嵌套json的形式返回。
        对于每一条提取到的记录，必须包含以下字段：
        - 序号: 文本中对应的序号
        - 企业名称: 企业的全称
        - 企业地址: 企业的具体地址
        
        如果当前段落中没有包含符合条件的企业表格信息，请返回一个空列表 []。
        """,
        output={
            "schema": {
                "companies": "list[{序号: string, 企业名称: string, 企业地址: string}]"
            }
        },
        model=default_model,
        max_batch_size=5,
        drop_keys=["text"]
    )
    operations.append(extract_op)
    operation_names.append("extract_company_info")
    
    # 2. CodeReduce操作：按URL合并去重（如果启用）
    code_reduce_op = CodeReduceOp(
        name="union_companies_by_url",
        type="code_reduce",
        reduce_key="url",
        code="""
def transform(items) -> list[dict]:
    import json
    all_unique_companies = []
    seen_fingerprints = set()
    for item in items:
        if "companies" in item and item["companies"]:
            for company in item["companies"]:
                fingerprint = tuple(sorted(company.items()))
                if fingerprint not in seen_fingerprints:
                    all_unique_companies.append(company)
                    seen_fingerprints.add(fingerprint)
    return {
        'url': items[0]['url'] if items else None,
        'companies': all_unique_companies
    }
            """
    )
    operations.append(code_reduce_op)
    operation_names.append("union_companies_by_url")
    
    # 定义Pipeline步骤
    step = PipelineStep(
        name="company_table_extraction",
        input="document_data",
        operations=operation_names
    )
    
    # 定义输出配置
    output_config = PipelineOutput(
        type="file",
        path=output_path,
        intermediate_dir=intermediate_dir
    )
    
    # 创建并运行Pipeline
    pipeline = Pipeline(
        name=f"{file_type}_company_extraction_pipeline",
        datasets={"document_data": dataset},
        operations=operations,
        steps=[step],
        output=output_config,
        default_model=default_model,
        system_prompt=system_prompt
    )
    
    # 执行Pipeline
    print(f"开始执行 {file_type.upper()} 文档提取...")
    cost, output = pipeline.run()

    # 删除cache文件
    if os.path.exists(os.path.join(os.path.expanduser("~"), ".cache", "docetl", "llm", "cache.db")):
        try:
            os.remove(os.path.join(os.path.expanduser("~"), ".cache", "docetl", "llm", "cache.db"))
            print("成功删除LLM缓存文件")
        except OSError as e:
            print(f"删除LLM缓存文件失败: {e}")
    if output_path and os.path.exists(output_path):
        try:
            os.remove(output_path)
            print(f"成功删除输出文件: {output_path}")
        except OSError as e:
            print(f"删除输出文件失败: {e}")
    if intermediate_dir and os.path.exists(intermediate_dir):
        try:
            # ignore_errors=True 可以忽略一些权限错误，或者保持默认 False 以便捕获异常
            shutil.rmtree(intermediate_dir)
            print(f"成功清理中间目录: {intermediate_dir}")
        except Exception as e:
            print(f"清理目录失败: {e}")
    
    return cost, output


# 使用示例
if __name__ == "__main__":
    
    # 示例1: 从PDF文件提取（手动配置OCR参数）
    # print("\n=== 示例1: PDF文件提取 ===")
    # cost1, output1 = extract_companies(
    #     input_data=[{"url": "https://gxw.xianyang.gov.cn/xwzx/tzgg/202312/P020231214614546500860.xlsx"}],
    #     file_type="pdf",
    #     parsing_config={
    #         "ocr_enabled": "True",
    #         "doc_per_page": "True",
    #         "lang": "ch"
    #     }
    # )
    
    # 示例2: 从Excel文件提取（使用memory类型，手动配置Excel参数）
    print("\n=== 示例2: Excel直接URL提取 ===")
    cost2, output2 = extract_companies(
        input_data=[{"url": "http://10.9.8.26:31010/open/ip_right_marking/43/e8/43e8ee8c0826905b77941d09178ddd92.xls"}],
        file_type="excel",
        parsing_config={
            "orientation": "row",
            "col_order": "企业名称,序号",
            "doc_per_sheet": "True",
            "max_rows_per_str": "50"
        }
    )
    print(f"执行成本: {cost2}")
    print(f"输出结果: {output2}")
    
    # 示例3: 从DOCX文件提取（手动配置分块参数）
    # print("\n=== 示例3: DOCX文件提取 ===")
    # cost3, output3 = extract_companies(
    #     input_data=[{"url": "https://gxw.xianyang.gov.cn/xwzx/tzgg/202312/P020231214614546500860.xlsx"}],
    #     file_type="docx",
    #     parsing_config={
    #         "lines_per_chunk": "100",
    #         "overlap_lines": "5"
    #     }
    # )
    
    # 示例4: 从HTML文件提取（手动配置HTML解析参数）
    # print("\n=== 示例4: HTML文件提取 ===")
    # cost4, output4 = extract_companies(
    #     input_data=[{"url": "https://gxw.xianyang.gov.cn/xwzx/tzgg/202312/P020231214614546500860.xlsx"}],
    #     file_type="html",
    #     parsing_config={
    #         "ocr_for_images": "True",
    #         "lang": "ch",
    #         "lines_per_chunk": "100",
    #         "overlap_lines": "5"
    #     }
    # )
