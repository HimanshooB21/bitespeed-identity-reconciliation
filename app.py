from flask import Flask, request, jsonify
from datetime import datetime
import sqlite3
from database import init_db

app = Flask(__name__)
init_db()

def get_db_connection():
    conn = sqlite3.connect('contacts.db')
    conn.row_factory = sqlite3.Row
    return conn

@app.route('/identify', methods=['POST'])
def identify():
    data = request.get_json()
    email = data.get('email')
    phone = data.get('phoneNumber')

    if not email and not phone:
        return jsonify({"error": "At least email or phoneNumber must be provided"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    # Find existing contacts matching email or phone
    cur.execute('''
        SELECT * FROM Contact
        WHERE email = ? OR phoneNumber = ?
    ''', (email, phone))
    existing_contacts = cur.fetchall()

    now = datetime.utcnow().isoformat()

    if not existing_contacts:
        # No match found, create new primary contact
        cur.execute('''
            INSERT INTO Contact (email, phoneNumber, linkPrecedence, createdAt, updatedAt)
            VALUES (?, ?, 'primary', ?, ?)
        ''', (email, phone, now, now))
        conn.commit()
        contact_id = cur.lastrowid
    else:
        # At least one match found
        # Get the earliest contact to be primary
        primary = None
        for contact in existing_contacts:
            if contact['linkPrecedence'] == 'primary':
                primary = contact
                break

        if not primary:
            primary = existing_contacts[0]

        # Check if this exact email or phone already exists
        already_exists = False
        for contact in existing_contacts:
            if contact['email'] == email and contact['phoneNumber'] == phone:
                already_exists = True
                break

        # If it’s a new combination, insert as secondary
        if not already_exists:
            cur.execute('''
                INSERT INTO Contact (email, phoneNumber, linkPrecedence, linkedId, createdAt, updatedAt)
                VALUES (?, ?, 'secondary', ?, ?, ?)
            ''', (email, phone, primary['id'], now, now))
            conn.commit()

        # Refresh full contact list (including just added one)
        cur.execute('''
            SELECT * FROM Contact
            WHERE id = ? OR linkedId = ?
        ''', (primary['id'], primary['id']))
        existing_contacts = cur.fetchall()

        contact_id = primary['id']

    emails = list(set([c['email'] for c in existing_contacts if c['email']]))
    phones = list(set([c['phoneNumber'] for c in existing_contacts if c['phoneNumber']]))
    secondary_ids = [c['id'] for c in existing_contacts if c['linkPrecedence'] == 'secondary']

    conn.close()

    return jsonify({
        "contact": {
            "primaryContactId": contact_id,
            "emails": emails,
            "phoneNumbers": phones,
            "secondaryContactIds": secondary_ids
        }
    })

if __name__ == '__main__':
    app.run(debug=True)