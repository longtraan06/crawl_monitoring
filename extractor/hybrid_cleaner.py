"""
Hybrid Extractor combining Mozilla Readability (via trafilatura.readability_lxml) and Trafilatura.
Ensures pristine text extraction with full preservation of section headings (H2, H3, H4)
and zero noise from headers, footers, comments, and related articles.
"""

import re
import html
from typing import Tuple, Optional
from bs4 import BeautifulSoup
import trafilatura

from .rules_120ask import extract_120ask
from ..config import MIN_CONTENT_CHARS

SITE_NAME_SUFFIXES = [
    r"\s*-\s*Báo Dân trí.*",
    r"\s*-\s*Tuổi Trẻ Online.*",
    r"\s*-\s*Báo Thanh Niên.*",
    r"\s*-\s*VnExpress.*",
    r"\s*-\s*Báo Nhân Dân.*",
    r"\s*-\s*Báo Lao Động.*",
    r"\s*\|\s*Vinmec.*",
    r"\s*\|\s*Medlatec.*",
    r"\s*-\s*Bệnh viện Tâm Anh.*",
    r"\s*-\s*Báo Sức khỏe & Đời sống.*",
    r"\s*_\s*120健康网.*",
    r"\s*_\s*快速问医生.*",
    r"\s*_\s*寻医问药网.*",
    r"\s*_\s*39健康网.*",
    r"\s*_\s*家庭医生在线.*"
]

class HybridCleaner:
    """Bộ trích xuất và chuẩn hóa Markdown y tế."""

    @staticmethod
    def clean_title(raw_title: str) -> str:
        """Làm sạch tiêu đề, cắt bỏ hậu tố tên báo/website."""
        if not raw_title:
            return ""
        title = html.unescape(raw_title.strip())
        for pattern in SITE_NAME_SUFFIXES:
            title = re.sub(pattern, "", title, flags=re.IGNORECASE)
        return title.strip()

    @classmethod
    def extract(cls, html_content: str, domain: str) -> Tuple[str, str, int]:
        """
        Trích xuất bài viết từ HTML.
        Trả về: (title, markdown_content, char_count)
        """
        if not html_content or len(html_content.strip()) < 50:
            return "", "", 0

        # 1. Nếu là diễn đàn hỏi đáp 120ask, áp dụng extractor chuyên biệt
        if "120ask.com" in domain:
            try:
                title, md = extract_120ask(html_content)
                if len(md) >= MIN_CONTENT_CHARS:
                    return cls.clean_title(title), md, len(md)
            except Exception:
                pass  # Fallback sang hybrid pipeline

        # 2. Pipeline Tổng Quát: Trích xuất Title từ Metadata hoặc H1
        meta = trafilatura.extract_metadata(html_content)
        title = meta.title if (meta and meta.title) else ""
        if not title:
            soup = BeautifulSoup(html_content, "html.parser")
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
            elif soup.title:
                title = soup.title.get_text(strip=True)
        title = cls.clean_title(title)

        # 3. Sử dụng Readability Container Detection + Trafilatura bọc <article> để giữ H2, H3
        md = ""
        try:
            tree = trafilatura.utils.load_html(html_content)
            doc = trafilatura.readability_lxml.Document(tree)
            summary_html = doc.summary()
            
            # Gói vào thẻ <article> để Trafilatura nhận diện bài viết đầy đủ
            wrapped_html = f"<!DOCTYPE html><html><body><article>{summary_html}</article></body></html>"
            md = trafilatura.extract(
                wrapped_html,
                output_format="markdown",
                include_links=False,
                include_images=False,
                include_tables=True,
                favor_recall=True
            ) or ""
        except Exception:
            md = ""

        # 4. Fallback: Nếu Readability trả về quá ngắn hoặc lỗi, chạy Trafilatura trực tiếp
        if not md or len(md.strip()) < MIN_CONTENT_CHARS:
            try:
                md = trafilatura.extract(
                    html_content,
                    output_format="markdown",
                    include_links=False,
                    include_images=False,
                    include_tables=True,
                    favor_recall=True
                ) or ""
            except Exception:
                md = ""

        # 5. Chuẩn hóa hậu kỳ
        final_md = cls._post_process_markdown(title, md)
        return title, final_md, len(final_md)

    @classmethod
    def _post_process_markdown(cls, title: str, md_text: str) -> str:
        """Chuẩn hóa markdown đầu ra: thêm tiêu đề H1 nếu thiếu, dọn dẹp khoảng trắng."""
        if not md_text:
            return ""

        lines = [line.rstrip() for line in md_text.splitlines()]
        
        # Xóa các dòng trống liên tiếp
        compact_lines = []
        prev_blank = False
        for line in lines:
            if not line:
                if not prev_blank:
                    compact_lines.append("")
                prev_blank = True
            else:
                compact_lines.append(line)
                prev_blank = False

        cleaned_body = "\n".join(compact_lines).strip()

        # Đảm bảo bài viết có Title ở đầu nếu chưa có H1
        if title and not cleaned_body.startswith("# "):
            cleaned_body = f"# {title}\n\n{cleaned_body}"

        return cleaned_body

# Khởi động trước trafilatura & dateparser để tránh deadlock import giữa các worker threads
try:
    trafilatura.extract("<html><body><p>Warmup text</p></body></html>")
except Exception:
    pass
