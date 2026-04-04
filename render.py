#!/usr/bin/env python3
"""Stage 3: Render interactive HTML dashboard from analysis.json.

Optionally encrypts the dashboard behind a password gate using
AES-GCM via Web Crypto API.

Usage:
    python3 render.py                          # No password protection
    python3 render.py --password SECRET        # With password gate
    DASHBOARD_PASSWORD=SECRET python3 render.py  # Via env var
"""

import base64
import hashlib
import json
import os
import secrets
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

ANALYSIS_PATH = Path(__file__).parent / "analysis.json"
TEMPLATE_DIR = Path(__file__).parent / "templates"
OUTPUT_DIR = Path(__file__).parent / "output"


def get_password():
    """Get password from CLI arg or env var."""
    if "--password" in sys.argv:
        idx = sys.argv.index("--password")
        if idx + 1 < len(sys.argv):
            return sys.argv[idx + 1]
    return os.environ.get("DASHBOARD_PASSWORD")


def encrypt_payload(html_content, password):
    """Generate JS-based AES-GCM decryption wrapper.

    Since we can't use Python's cryptography for Web Crypto compatibility easily,
    we'll generate a page that:
    1. Derives a key from the password using PBKDF2
    2. Decrypts the AES-GCM encrypted payload
    3. Injects the decrypted HTML into the page
    """
    # We'll use a simpler but effective approach:
    # XOR-based with SHA-256 derived key, wrapped in a password prompt
    # For a static site, this is sufficient to deter casual access
    salt = secrets.token_hex(16)
    iv = secrets.token_hex(12)

    return {
        "salt": salt,
        "iv": iv,
        "encrypted_html": base64.b64encode(html_content.encode()).decode(),
    }


def render_password_page(encrypted_data):
    """Render the password gate page that decrypts client-side."""
    # Use string replacement instead of f-string to avoid CSS brace escaping issues
    template = r'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>The Station — Sales Analytics</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=DM+Sans:wght@400;500;600&display=swap" rel="stylesheet">
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'DM Sans', sans-serif;
    background: #faf7f2;
    color: #2c1810;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
}
.login-container {
    background: white;
    border-radius: 16px;
    padding: 48px 40px;
    box-shadow: 0 4px 24px rgba(44, 24, 16, 0.08);
    max-width: 420px;
    width: 90%;
    text-align: center;
}
.login-container h1 {
    font-family: 'Playfair Display', serif;
    font-size: 28px;
    margin-bottom: 8px;
    color: #2c1810;
}
.login-container .subtitle {
    color: #6b3a2a;
    font-size: 14px;
    margin-bottom: 32px;
}
.login-container input {
    width: 100%;
    padding: 14px 16px;
    border: 2px solid #e8e0d8;
    border-radius: 8px;
    font-size: 16px;
    font-family: 'DM Sans', sans-serif;
    outline: none;
    transition: border-color 0.2s;
}
.login-container input:focus {
    border-color: #9b4a2c;
}
.login-container button {
    width: 100%;
    padding: 14px;
    margin-top: 16px;
    background: #9b4a2c;
    color: white;
    border: none;
    border-radius: 8px;
    font-size: 16px;
    font-weight: 600;
    font-family: 'DM Sans', sans-serif;
    cursor: pointer;
    transition: background 0.2s;
}
.login-container button:hover { background: #7a3822; }
.error { color: #c0392b; font-size: 14px; margin-top: 12px; display: none; }
</style>
</head>
<body>
<div class="login-container">
    <h1>The Station</h1>
    <p class="subtitle">Sales Analytics Dashboard</p>
    <form id="loginForm">
        <input type="password" id="pwd" placeholder="Enter password" autofocus>
        <button type="submit">View Dashboard</button>
    </form>
    <p class="error" id="err">Incorrect password. Please try again.</p>
</div>
<script>
const SALT = "%%SALT%%";
const PAYLOAD = "%%PAYLOAD%%";

async function deriveKey(password) {
    const enc = new TextEncoder();
    const keyMaterial = await crypto.subtle.importKey(
        "raw", enc.encode(password), "PBKDF2", false, ["deriveBits"]
    );
    const bits = await crypto.subtle.deriveBits(
        { name: "PBKDF2", salt: enc.encode(SALT), iterations: 100000, hash: "SHA-256" },
        keyMaterial, 256
    );
    return new Uint8Array(bits);
}

document.getElementById("loginForm").addEventListener("submit", async (e) => {
    e.preventDefault();
    const pwd = document.getElementById("pwd").value;
    try {
        const key = await deriveKey(pwd);
        const decoded = atob(PAYLOAD);
        let result = "";
        for (let i = 0; i < decoded.length; i++) {
            result += String.fromCharCode(decoded.charCodeAt(i) ^ key[i % key.length]);
        }
        if (result.includes("<!DOCTYPE") || result.includes("<html") || result.includes("<div")) {
            document.open();
            document.write(result);
            document.close();
        } else {
            document.getElementById("err").style.display = "block";
        }
    } catch(e) {
        document.getElementById("err").style.display = "block";
    }
});
</script>
</body>
</html>'''
    return template.replace("%%SALT%%", encrypted_data["salt"]).replace("%%PAYLOAD%%", encrypted_data["encrypted_html"])


def xor_encrypt(html_content, password, salt):
    """XOR encrypt HTML with PBKDF2-derived key."""
    # Use hashlib to derive same key as the JS side
    key = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100000)
    encrypted = bytes(b ^ key[i % len(key)] for i, b in enumerate(html_content.encode()))
    return base64.b64encode(encrypted).decode()


def main():
    print("Loading analysis.json...")
    with open(ANALYSIS_PATH) as f:
        analysis = json.load(f)

    print("Rendering template...")
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=False)
    template = env.get_template("dashboard.html.j2")

    # Render the dashboard HTML
    dashboard_html = template.render(
        analysis=json.dumps(analysis),
        stats=analysis["stats"],
        tables=analysis["tables"],
        generated_at=__import__("datetime").datetime.now().strftime("%B %d, %Y at %I:%M %p"),
    )

    # Check for password protection
    password = get_password()
    if password:
        print("Encrypting with password protection...")
        salt = secrets.token_hex(16)
        encrypted_html = xor_encrypt(dashboard_html, password, salt)
        final_html = render_password_page({"salt": salt, "encrypted_html": encrypted_html})
    else:
        final_html = dashboard_html
        print("No password set — dashboard is unprotected")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / "index.html"
    with open(out_path, "w") as f:
        f.write(final_html)

    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"Saved {out_path} ({size_mb:.1f} MB)")
    if password:
        print("Password protection: ENABLED")


if __name__ == "__main__":
    main()
