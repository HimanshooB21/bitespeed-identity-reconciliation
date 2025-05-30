import sqlite3

def init_db():
    conn = sqlite3.connect('contacts.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS Contact (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phoneNumber TEXT,
            email TEXT,
            linkedId INTEGER,
            linkPrecedence TEXT CHECK(linkPrecedence IN ('primary', 'secondary')),
            createdAt TEXT,
            updatedAt TEXT,
            deletedAt TEXT
        )
    ''')
    conn.commit()
    conn.close()
