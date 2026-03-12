# Quote Checker App

Streamlit app for validating solar quote assumptions against calculated results.

## Included Files

- `quote_checker_app.py` - Main Streamlit app
- `requirements.txt` - Python dependencies
- `runtime.txt` - Python runtime version for cloud deploys
- `.gitignore` - Excludes local/sensitive files

## Local Run

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python -m streamlit run quote_checker_app.py
```

## Publish To GitHub (GitHub Desktop)

1. Open **GitHub Desktop** and sign in.
2. Click **File -> Add local repository...** and choose this folder.
3. If prompted, choose **Create a Repository**.
4. Repository name: `quote-checker-app`.
5. Commit message: `Initial commit - quote checker app`.
6. Click **Commit to main**.
7. Click **Publish repository**.
8. Choose **Public** or **Private** and confirm.

## Publish To GitHub (Command Line)

Run `setup_github.ps1` from this folder.

Example (create local git repo + first commit only):

```powershell
.\setup_github.ps1
```

Example (also connect and push to GitHub):

```powershell
.\setup_github.ps1 -RepoUrl https://github.com/<your-user>/<your-repo>.git
```

## Deploy To Streamlit Community Cloud

1. Go to Streamlit Community Cloud and click **New app**.
2. Select your GitHub repo and branch.
3. Set **Main file path** to `quote_checker_app.py`.
4. Click **Deploy**.

## Important

- Do not commit customer PDFs/CSVs/XLSX files.
- Do not commit extracted quote text files.
- Keep customer data uploads local only.
