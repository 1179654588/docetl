import os

# 默认系统环境配置
os.environ['ARK_API_KEY'] = 'afda58f0-0e4e-423d-904e-de22e0c6854b'
os.environ['OPENAI_API_BASE'] = 'https://ark.cn-beijing.volces.com/api/v3'
os.environ['OPENAI_API_KEY'] = 'afda58f0-0e4e-423d-904e-de22e0c6854b'
os.environ['MODEL_NAME'] = 'doubao-seed-1-8-251228'
os.environ['PYTHONUTF8'] = '1'

from docetl.api import Pipeline, Dataset, MapOp, UnnestOp, ResolveOp, PipelineStep, PipelineOutput

# 1. 定义数据集 - 包含 PDF 解析配置
dataset = Dataset(
    type="file",
    path=r"D:\code\doc-etl\docetl\dataset\pdf_output.json",
    source="local",
    parsing=[
        {
            "input_key": "url",
            "function": "paddleocr_pdf_to_string",
            "output_key": "text",
            "ocr_enabled": "true",
            "doc_per_page": "true",
            "lang": "ch"
        }
    ]
)

# 2. 定义操作 (Operations)
operations = [
    # 映射操作：从文本段落中提取企业信息
    MapOp(
        name="extract_company_info",
        type="map",
        prompt="""
        请分析以下从 PDF 中提取的段落文本：
        {{ input.text }}
        
        你的任务是识别并提取文本中隐含的表格数据。请提取出所有提及的企业信息，并以列表嵌套json的形式返回。
        对于每一条提取到的记录，必须包含以下字段：
        - 序号: 文本中对应的序号
        - 企业名称: 企业的全称或简称
        - 企业地址: 企业的具体地址
        - 备注: 相关的补充信息或备注，如果没有则留空
        
        如果当前段落中没有包含符合条件的企业表格信息，请返回一个空列表 []。
        """,
        output={
            "schema": {
                "companies": "list[{序号: string, 企业名称: string, 企业地址: string, 备注: string}]"
            }
        },
        model="volcengine/doubao-seed-1-8-251228",
        max_batch_size=5,
        drop_keys=["text"]
    ),
    
    # 展开操作：将企业列表拆分为单条记录
    UnnestOp(
        name="unnest_companies",
        type="unnest",
        unnest_key="companies"
    ),
    
    # 解析操作：解决跨页或重复提取的企业去重问题
    ResolveOp(
        name="resolve_duplicate_companies",
        type="resolve",
        blocking_keys=["company_name"],
        blocking_threshold=0.85,
        comparison_prompt="""
        请比较以下两条提取出的企业信息：
        企业 1: {{ input1.company_name }} (地址: {{ input1.company_address }})
        企业 2: {{ input2.company_name }} (地址: {{ input2.company_address }})
        请判断这两条记录是否指向同一家企业（有时PDF跨页会导致同一家企业被提取两次）？
        """,
        embedding_model="text-embedding-3-small",
        output={
            "schema": {
                "index": "str",
                "company_name": "str",
                "company_address": "str",
                "remarks": "str"
            }
        },
        resolution_prompt="""
        以下是几条被判定为同一家企业的匹配记录：
        {% for entry in inputs %}
        记录 {{ loop.index }}:
        - 序号: {{ entry.index }}
        - 名称: {{ entry.company_name }}
        - 地址: {{ entry.company_address }}
        - 备注: {{ entry.remarks }}
        {% endfor %}
        
        请将它们合并为一条最完整、准确的记录。
        规则：
        1. 使用最完整、最正式的企业名称和地址。
        2. 汇总所有的备注信息（如果不冲突）。
        3. 序号如果不同，优先保留最靠前的序号。
        """
    )
]

# 3. 定义 Pipeline 步骤
# 注意：根据你的 YAML，目前步骤中仅启用了 extract_company_info
step = PipelineStep(
    name="company_table_extraction",
    input="pdf_paragraphs",
    operations=[
        "extract_company_info"
        # "unnest_companies",          # 如果需要去重，请取消注释
        # "resolve_duplicate_companies" # 如果需要去重，请取消注释
    ]
)

# 4. 定义输出配置
output = PipelineOutput(
    type="file",
    path=r"D:\code\doc-etl\docetl\output\pdf_output.json",
    intermediate_dir=r"D:\code\doc-etl\docetl\output\intermediates"
)

# 5. 定义系统提示词
system_prompt = {
    "dataset_description": "从 PDF 解析出的段落文本，包含企业相关的表格信息。",
    "persona": "一个专业的文档数据提取专家，擅长从非结构化文本中精准还原表格数据。"
}

# 6. 创建并运行 Pipeline
pipeline = Pipeline(
    name="company_info_pipeline",
    datasets={"pdf_paragraphs": dataset},
    operations=operations,
    steps=[step],
    output=output,
    default_model="volcengine/doubao-seed-1-8-251228",
    system_prompt=system_prompt
)

# 运行（由于 YAML 中提到了优化器配置，但在 Python API 中通常通过配置传递给 run）
cost, output = pipeline.run()
print(f"Pipeline 执行完成。总花费: ${cost:.2f}")
print(f"Pipeline 输出: {output}")
