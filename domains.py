"""
Domain registry, categorizations, and corpus metadata for ViBioMIR crawler.
"""

import json
from pathlib import Path
from typing import Set, Dict, List, Any

# Nhóm 1: 86 domain hoạt động hoàn toàn ổn định (chiếm 3,638,908 URLs - 82.80% corpus)
GROUP_1_STABLE_DOMAINS: Set[str] = {
    "www.cnkang.com",
    "www.120ask.com",
    "www.familydoctor.com.cn",
    "www.a-hospital.com",
    "suckhoecongdongonline.vn",
    "zhongyibaodian.net",
    "suckhoedoisong.vn",
    "www.zydcd.com",
    "thanhnien.vn",
    "www.youlai.cn",
    "laodong.vn",
    "baby.39.net",
    "phunusuckhoe.giadinhonline.vn",
    "www.vinmec.com",
    "medlatec.vn",
    "qihuangzhishu.com",
    "jbk.39.net",
    "giadinhonline.vn",
    "baohaiphong.vn",
    "baonghean.vn",
    "vietnamnet.vn",
    "care.39.net",
    "test.pmphai.com",
    "baodanang.vn",
    "tiemchunglongchau.com.vn",
    "fitness.39.net",
    "baocantho.com.vn",
    "hellobacsi.com",
    "woman.39.net",
    "food.39.net",
    "baoangiang.com.vn",
    "fk.39.net",
    "vov.vn",
    "cancer.39.net",
    "khoahocphothong.vn",
    "jb39.com",
    "youmed.vn",
    "vov2.vov.vn",
    "nk.39.net",
    "phuyen.baodaklak.vn",
    "heart.39.net",
    "suckhoeviet.org.vn",
    "baoquangtri.vn",
    "man.39.net",
    "nhandan.vn",
    "baovinhlong.com.vn",
    "gan.39.net",
    "tamanhhospital.vn",
    "tnb.39.net",
    "wei.39.net",
    "tuoitre.vn",
    "baidianfeng.familydoctor.com.cn",
    "www.qdnd.vn",
    "www.vietnamplus.vn",
    "baogialai.com.vn",
    "shen.39.net",
    "ek.39.net",
    "tienphong.vn",
    "thaythuocvietnam.vn",
    "baoquangninh.vn",
    "bachmai.gov.vn",
    "www.baidu.com",
    "baochinhphu.vn",
    "dantri.com.vn",
    "gk.39.net",
    "vnexpress.net",
    "baolangson.vn",
    "www.sggp.org.vn",
    "gc.39.net",
    "pf.39.net",
    "health.people.com.cn",
    "baophutho.vn",
    "hanoimoi.vn",
    "benhviennhitrunguong.gov.vn",
    "baotayninh.vn",
    "sj.39.net",
    "baothanhhoa.vn",
    "byby.39.net",
    "benhvienvietduc.org",
    "www.pharmacity.vn",
    "www.familydoctor.cn",
    "qy.familydoctor.com.cn",
    "ask.familydoctor.com.cn",
    "mega.vietnamplus.vn",
    "yanglao.familydoctor.com.cn",
    "fk.99.com.cn",
}

# Nhóm 2: 2 domain cần cấu hình kỹ thuật riêng (chiếm 132,939 URLs - 3.02% corpus)
GROUP_2_SPECIAL_DOMAINS: Set[str] = {
    "nhathuoclongchau.com.vn",  # Bật Cloudflare Bot Management nếu gửi request quá nhanh
    "www.wujue.com",            # Server TQ độ trễ cao, cần retry x3 và timeout > 35s
}

# Nhóm 3: 9 domain chết / bị chặn hoàn toàn (chiếm 622,871 URLs - 14.17% corpus)
GROUP_3_DEAD_DOMAINS: Set[str] = {
    "ask.39.net",                           # Connection Reset / BoringSSL handshake drop
    "zysjonline.com",                       # Cloudflare Turnstile Captcha chặn 100%
    "bingli.iiyi.com",                      # HTTP 521: Server gốc bệnh án sập hẳn
    "article.iiyi.com",                     # HTTP 521: Server gốc bài viết sập hẳn
    "www.msdmanuals.cn",                    # Bị ngắt kết nối (ERR_CONNECTION_RESET)
    "pmc-ecm-healthblog.beta.pharmacity.io",# HTTP 403: Môi trường staging nội bộ đã đóng
    "v.familydoctor.com.cn",                # Timeout > 20s (Subdomain video cũ dừng hoạt động)
    "special.vietnamplus.vn",               # HTTP 403: Chuyên đề cũ đóng public access
    "ypk.familydoctor.com.cn",              # Timeout > 20s (Subdomain tra cứu thuốc cũ ngừng hoạt động)
}

DOMAIN_NOTES: Dict[str, str] = {
    "ask.39.net": "BoringSSL reset / Drop IP ngoài Trung Quốc",
    "zysjonline.com": "Cloudflare Turnstile Captcha chặn 100%",
    "bingli.iiyi.com": "HTTP 521 (Server gốc bệnh án đã chết)",
    "article.iiyi.com": "HTTP 521 (Server gốc bài viết đã chết)",
    "www.msdmanuals.cn": "ERR_CONNECTION_RESET / Tường lửa",
    "pmc-ecm-healthblog.beta.pharmacity.io": "HTTP 403 (Staging beta nội bộ đã đóng)",
    "v.familydoctor.com.cn": "Timeout > 20s (Dịch vụ video cũ đã tắt)",
    "special.vietnamplus.vn": "HTTP 403 (Trang tương tác cũ khóa public)",
    "ypk.familydoctor.com.cn": "Timeout > 20s (Tra cứu thuốc cũ không phản hồi)",
    "nhathuoclongchau.com.vn": "Cloudflare Bot Defense - Cần rate-limit 2-3 req/s",
    "www.wujue.com": "Server TQ độ trễ cao - Cần timeout > 35s & retry x3"
}

def get_domain_group(domain: str) -> int:
    """Trả về nhóm của domain (1: Stable, 2: Special, 3: Dead, 0: Unknown)."""
    domain = domain.lower().strip()
    if domain in GROUP_1_STABLE_DOMAINS:
        return 1
    if domain in GROUP_2_SPECIAL_DOMAINS:
        return 2
    if domain in GROUP_3_DEAD_DOMAINS:
        return 3
    return 0

def is_domain_allowed(
    domain: str,
    target_groups: List[int],
    target_domains: Optional[Set[str]] = None
) -> bool:
    """Kiểm tra domain có được phép crawl theo nhóm hoặc danh sách domain chỉ định hay không."""
    domain = domain.lower().strip()
    if target_domains and domain in target_domains:
        return True
    group = get_domain_group(domain)
    return group in target_groups

def is_allowed_domain(domain: str, include_group_2: bool = False) -> bool:
    """Hàm tương thích cũ cho pipeline."""
    target_groups = [1, 2] if include_group_2 else [1]
    return is_domain_allowed(domain, target_groups)

def get_all_domains_metadata() -> List[Dict[str, Any]]:
    """Trả về danh sách 97 domain kèm thông số số lượng URL trong corpus và phân nhóm."""
    stats_file = Path(__file__).resolve().parent.parent / "unique_website_stats.json"
    results = []
    
    if stats_file.exists():
        try:
            with open(stats_file, "r", encoding="utf-8") as f:
                raw_stats = json.load(f)
            for item in raw_stats:
                d = item["domain"]
                group = get_domain_group(d)
                results.append({
                    "domain": d,
                    "website_url": item.get("website_url", f"https://{d}"),
                    "url_count": item.get("url_count", 0),
                    "percentage": item.get("percentage", 0.0),
                    "group_id": group,
                    "group_name": f"Nhóm {group}" if group > 0 else "Chưa phân nhóm",
                    "note": DOMAIN_NOTES.get(d, "Hoạt động bình thường" if group == 1 else "")
                })
        except Exception:
            pass

    return results
