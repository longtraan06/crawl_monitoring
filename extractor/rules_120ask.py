"""
Specialized high-fidelity extractor for 120ask.com doctor consultation forum.
"""

import re
from typing import Tuple
from bs4 import BeautifulSoup

def extract_120ask(html: str) -> Tuple[str, str]:
    """
    Trích xuất cấu trúc câu hỏi bệnh nhân và tư vấn của bác sĩ từ 120ask.com.
    Trả về: (title, markdown)
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # 1. Trích xuất tiêu đề
    h1 = soup.find("h1", id="d_askH1") or soup.find("h1")
    title = h1.get_text(strip=True) if h1 else ""
    if not title and soup.title:
        title = soup.title.get_text(strip=True)
    title = title.replace("_120健康网", "").replace("_快速问医生", "").strip()

    # 2. Trích xuất câu hỏi / mô tả triệu chứng của bệnh nhân
    q_el = soup.find(class_="b_askcont") or soup.find(class_="b_askdesc") or soup.find(class_="f-q-main")
    q_text = q_el.get_text("\n", strip=True) if q_el else ""
    q_text = re.sub(r"^(健康咨询描述|患者性别|患者年龄|问题描述|病情描述)[：:]\s*", "", q_text)
    q_text = re.sub(r"^BR\s*>", "", q_text, flags=re.I).strip()

    # 3. Trích xuất các câu trả lời của bác sĩ
    answer_items = soup.find_all(class_="b_answerli") or soup.find_all(class_="crazy_doctor")
    answers = []
    for li in answer_items:
        doc_header = li.find(class_="b_answertl") or li.find(class_="b_answertop")
        doc_info = doc_header.get_text(" ", strip=True) if doc_header else "Bác sĩ"
        doc_info = doc_info.replace("微信扫一扫，随时问医生", "").strip()

        ans_body = (
            li.find(class_="crazy_new") or 
            li.find(class_="b_anscont_cont") or 
            li.find(class_="b_anscont")
        )
        if ans_body:
            ans_text = ans_body.get_text("\n", strip=True)
            if "您的浏览器不支持 audio 元素" in ans_text:
                ans_text = "[Bác sĩ tư vấn bằng bản ghi âm Voice Audio - không có văn bản]"
            answers.append({"doctor": doc_info, "answer": ans_text})

    # 4. Tổ chức định dạng Markdown hoàn chỉnh
    parts = []
    if title:
        parts.append(f"# {title}\n")
    if q_text:
        parts.append(f"## Mô tả câu hỏi của bệnh nhân\n{q_text}\n")
    if answers:
        parts.append(f"## Ý kiến tư vấn của bác sĩ ({len(answers)} câu trả lời)")
        for i, a in enumerate(answers, 1):
            parts.append(f"### Bác sĩ {i}: {a['doctor']}\n{a['answer']}\n")
    else:
        parts.append("## Ý kiến tư vấn của bác sĩ\n*(Chưa có câu trả lời nào từ bác sĩ cho câu hỏi này)*\n")

    md = "\n".join(parts).strip()
    return title, md
