import pandas as pd
import streamlit as st
import io
import os
from rapidfuzz import process, fuzz

# ─── Cấu hình cột chuẩn & alias ──────────────────────────────────────────
COLUMN_ALIASES = {
    "Họ và tên":  ["họ và tên", "ho va ten", "họ tên", "ho ten", "tên", "ten", "full name", "name", "họ & tên"],
    "Lớp":        ["lớp", "lop", "lớp học", "lop hoc", "class", "khối", "khoi"],
    "Giới tính":  ["giới tính", "gioi tinh", "gender", "sex"],
    "Ngày sinh":  ["ngày sinh", "ngay sinh", "ngày tháng năm sinh", "ngay thang nam sinh",
                   "dob", "date of birth", "birthday", "sinh ngày"],
    "Địa chỉ":    ["địa chỉ", "dia chi", "address", "nơi ở", "noi o"],
    "Dân tộc":    ["dân tộc", "dan toc", "ethnicity"],
    "Mã HS":      ["mã hs", "ma hs", "mã học sinh", "ma hoc sinh", "student id", "id"],
    "Điện thoại": ["điện thoại", "dien thoai", "phone", "sđt", "sdt", "số điện thoại"],
    "Email":      ["email", "e-mail", "mail"],
}

# Thứ tự cột ưu tiên trong file output
PRIORITY_COLS = ["Họ và tên", "Lớp", "Giới tính", "Ngày sinh"]

def normalize(text: str) -> str:
    """Chuyển về chữ thường, bỏ khoảng trắng thừa."""
    return str(text).strip().lower()

def map_column(col_name: str, threshold: int = 75) -> str | None:
    """Trả về tên cột chuẩn nếu khớp, ngược lại None."""
    col_norm = normalize(col_name)
    for standard, aliases in COLUMN_ALIASES.items():
        if col_norm in aliases or col_norm == normalize(standard):
            return standard
        # fuzzy match
        match, score, _ = process.extractOne(col_norm, aliases, scorer=fuzz.token_sort_ratio)
        if score >= threshold:
            return standard
    return None

def detect_header_row(df_raw: pd.DataFrame, max_scan: int = 10) -> int:
    """Tìm hàng chứa header thực sự (có nhiều cột khớp alias nhất)."""
    best_row, best_score = 0, 0
    for i in range(min(max_scan, len(df_raw))):
        row = df_raw.iloc[i]
        score = sum(1 for v in row if map_column(str(v)) is not None)
        if score > best_score:
            best_score, best_row = score, i
    return best_row

def read_sheet_smart(xl: pd.ExcelFile, sheet_name: str) -> pd.DataFrame:
    """Đọc 1 sheet, tự phát hiện header row, mapping cột chuẩn."""
    df_raw = xl.parse(sheet_name, header=None)
    header_row = detect_header_row(df_raw)

    df = xl.parse(sheet_name, header=header_row)
    df.columns = [str(c).strip() for c in df.columns]

    # Đổi tên cột sang tên chuẩn
    rename_map = {}
    for col in df.columns:
        standard = map_column(col)
        if standard:
            rename_map[col] = standard

    df = df.rename(columns=rename_map)
    df["__sheet__"] = sheet_name  # dùng nội bộ để debug

    # Bỏ các hàng hoàn toàn trống
    df = df.dropna(how="all")
    return df

def merge_sheets(file_bytes: bytes) -> tuple[pd.DataFrame, list[str]]:
    """Gộp tất cả sheet thành 1 DataFrame, chuẩn hoá cột."""
    xl = pd.ExcelFile(file_bytes)
    logs = []
    frames = []

    for sheet in xl.sheet_names:
        try:
            df = read_sheet_smart(xl, sheet)
            logs.append(f"✅ Sheet **{sheet}**: {len(df)} dòng, cột → {list(df.columns)}")
            frames.append(df)
        except Exception as e:
            logs.append(f"⚠️ Sheet **{sheet}**: lỗi – {e}")

    if not frames:
        return pd.DataFrame(), logs

    merged = pd.concat(frames, ignore_index=True, sort=False)

    # Định dạng cột Ngày sinh thành dd/mm/yyyy (nếu có)
    if "Ngày sinh" in merged.columns:
        def fmt_date(val):
            if pd.isna(val) or str(val).strip() == "":
                return val
            # Nếu đã là datetime/Timestamp
            if hasattr(val, "strftime"):
                return val.strftime("%d/%m/%Y")
            # Nếu là chuỗi, thử parse
            try:
                return pd.to_datetime(val, dayfirst=True).strftime("%d/%m/%Y")
            except Exception:
                return val  # giữ nguyên nếu không parse được
        merged["Ngày sinh"] = merged["Ngày sinh"].apply(fmt_date)

    # Đảm bảo cột "Giới tính" luôn tồn tại (để trống nếu không có trong file gốc)
    if "Giới tính" not in merged.columns:
        merged["Giới tính"] = ""

    # Xây dựng thứ tự cột đầu ra:
    # 1. Các cột ưu tiên: Họ và tên, Lớp, Giới tính, Ngày sinh (luôn đứng đầu)
    # 2. Các cột chuẩn khác có trong dữ liệu
    # 3. Các cột không thuộc chuẩn (giữ nguyên từ file gốc)
    all_standard = list(COLUMN_ALIASES.keys())
    remaining_standard = [c for c in all_standard if c not in PRIORITY_COLS and c in merged.columns]
    other_cols = [c for c in merged.columns if c not in all_standard and c != "__sheet__"]

    final_order = PRIORITY_COLS + remaining_standard + other_cols
    if "__sheet__" in merged.columns:
        final_order.append("__sheet__")

    merged = merged[final_order]

    return merged, logs

# ─── Giao diện Streamlit ───────────────────────────────────────────────────────
st.set_page_config(page_title="Gộp Sheet Excel", page_icon="🐍", layout="centered")
st.title("🙃 Gộp nhiều Sheet trong 1 file Excel")
st.caption("Tự động nhận diện & chuẩn hoá cột: Lớp, Họ tên, Ngày sinh, Giới tính…")

uploaded = st.file_uploader("⬆️ Tải lên file Excel (.xlsx)", type=["xlsx", "xls"])

if uploaded:
    base_name = os.path.splitext(uploaded.name)[0]
    output_name = f"{base_name}_đã gộp.xlsx"

    st.info(f"📂 File: **{uploaded.name}** — File đầu ra sẽ là: **{output_name}**")

    with st.spinner("Đang xử lý…"):
        file_bytes = uploaded.read()
        merged_df, logs = merge_sheets(io.BytesIO(file_bytes))

    # Log chi tiết
    with st.expander("🔍 Chi tiết từng sheet", expanded=False):
        for log in logs:
            st.markdown(log)

    if merged_df.empty:
        st.error("Không đọc được dữ liệu từ file. Vui lòng kiểm tra lại.")
    else:
        # Ẩn cột nội bộ khi hiển thị
        display_df = merged_df.drop(columns=["__sheet__"], errors="ignore")

        st.success(f"✅ Gộp thành công **{len(display_df)} dòng**, **{len(display_df.columns)} cột**")
        st.dataframe(display_df, use_container_width=True)

        # Xuất Excel
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            display_df.to_excel(writer, index=False, sheet_name="Đã gộp")
        buf.seek(0)

        st.download_button(
            label="⬇️ Tải về file đã gộp",
            data=buf,
            file_name=output_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
