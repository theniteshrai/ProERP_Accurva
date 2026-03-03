import os
import sqlite3
import psycopg
from psycopg import rows
import random
from datetime import datetime, timedelta

DB_NAME = 'proerp.db'
DATABASE_URL = os.environ.get("DATABASE_URL")
IS_POSTGRES = bool(DATABASE_URL)

def get_db():
    if IS_POSTGRES:
        conn = psycopg.connect(DATABASE_URL, row_factory=rows.dict_row)
    else:
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
    return conn

def execute_query(cursor, sql, params=None):
    if IS_POSTGRES and params:
        sql = sql.replace("?", "%s")
    cursor.execute(sql, params)
    return cursor

def seed_dummy_data():
    conn = get_db()
    c = conn.cursor()
    
    # Get organisation ID
    execute_query(c, 'SELECT id FROM organisations LIMIT 1')
    org = c.fetchone()
    try:
        org_id = org['id'] if org else 1
    except (TypeError, KeyError, IndexError):
        org_id = org[0] if org else 1
    
    # Delete existing data
    execute_query(c, 'DELETE FROM invoice_items')
    execute_query(c, 'DELETE FROM invoices')
    execute_query(c, 'DELETE FROM transactions')
    execute_query(c, 'DELETE FROM expenses')
    execute_query(c, 'DELETE FROM purchase_order_items')
    execute_query(c, 'DELETE FROM purchase_orders')
    execute_query(c, 'DELETE FROM quotation_items')
    execute_query(c, 'DELETE FROM quotations')
    execute_query(c, 'DELETE FROM parties')
    execute_query(c, 'DELETE FROM items')
    conn.commit()
    
    print("Deleted all existing records...")
    
    # Seed Items
    items = [
        ('Laptop Computer', '8471', 'LAP-001', 'PCS', 45000, 18, 50),
        ('Desktop Computer', '8471', 'DES-001', 'PCS', 35000, 18, 30),
        ('Wireless Mouse', '8471', 'MOU-001', 'PCS', 599, 18, 200),
        ('Keyboard', '8471', 'KEY-001', 'PCS', 899, 18, 150),
        ('Monitor 22 inch', '8528', 'MON-001', 'PCS', 12000, 18, 75),
        ('Monitor 27 inch', '8528', 'MON-002', 'PCS', 18000, 18, 40),
        ('Printer', '8443', 'PRT-001', 'PCS', 8500, 18, 25),
        ('USB Cable', '8544', 'USB-001', 'PCS', 150, 18, 500),
        ('Pen Drive 16GB', '8523', 'PEN-001', 'PCS', 350, 18, 300),
        ('Pen Drive 32GB', '8523', 'PEN-002', 'PCS', 550, 18, 250),
        ('External HDD 1TB', '8471', 'HDD-001', 'PCS', 4500, 18, 60),
        ('SSD 256GB', '8471', 'SSD-001', 'PCS', 3500, 18, 80),
        ('RAM 8GB', '8471', 'RAM-001', 'PCS', 2500, 18, 100),
        ('Router', '8517', 'ROU-001', 'PCS', 1500, 18, 45),
        ('Webcam', '8525', 'WEB-001', 'PCS', 1200, 18, 60),
        ('Headphones', '8518', 'HEAD-001', 'PCS', 800, 18, 120),
        ('UPS 600VA', '8504', 'UPS-001', 'PCS', 3500, 18, 35),
        ('Cable LAN', '8544', 'LAN-001', 'PCS', 250, 18, 200),
        ('Power Strip', '8536', 'PWR-001', 'PCS', 450, 18, 100),
        ('LED Bulb 9W', '8539', 'LED-001', 'PCS', 120, 18, 500),
    ]
    
    item_ids = []
    for item in items:
        execute_query(c, '''INSERT INTO items (name, hsn_code, sku, unit, rate, gst_rate, opening_stock, organisation_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (*item, org_id))
        item_ids.append(c.lastrowid if not IS_POSTGRES else None)
    
    if IS_POSTGRES:
        execute_query(c, 'SELECT id FROM items ORDER BY id DESC LIMIT ?', (len(items),))
        item_ids = [row['id'] for row in reversed(c.fetchall())]

    conn.commit()
    print(f"Created {len(items)} items")
    
    # Seed Parties (Customers and Vendors)
    parties = [
        # Customers
        ('Apex Solutions Pvt Ltd', 'customer', '27AAPCS1234A1Z5', 'APECS1234R', '02223456789', 'info@apexsolutions.com', '101, Business Park, Andheri East', 'Maharashtra', 'Mumbai', 'Maharashtra', 50000),
        ('Tech Vision Industries', 'customer', '29AAGCT5678A1Z3', 'TECVI5678P', '08023456789', 'accounts@techvision.com', '50, Tech City, Electronic City', 'Karnataka', 'Bangalore', 'Karnataka', 75000),
        ('Global Enterprises', 'customer', '07AAMFG9012A1Z4', 'GLOBE9012K', '01123456789', 'purchase@globalent.com', '200, Connaught Place', 'Delhi', 'New Delhi', 'Delhi', 60000),
        ('Prime Services Ltd', 'customer', '27AABCU4567A1Z5', 'PRIS4567F', '02234567890', 'orders@primeservices.com', '75, Worli Sea Face', 'Maharashtra', 'Mumbai', 'Maharashtra', 45000),
        ('Sunrise Trading Co', 'customer', '33AAGCT7890A1Z2', 'SUNR7890M', '04423456789', 'buy@sunrisetrading.com', '88, T Nagar', 'Tamil Nadu', 'Chennai', 'Tamil Nadu', 35000),
        ('Blue Sky Systems', 'customer', '19AABCS2345A1Z8', 'BLUS2345Q', '03323456789', 'sales@blueskysys.com', '42, Salt Lake Sector V', 'West Bengal', 'Kolkata', 'West Bengal', 40000),
        ('Ocean View Pvt Ltd', 'customer', '24AAGCD6789A1Z1', 'OCEV6789L', '07923456789', 'purchase@oceanview.com', '155, SG Highway', 'Gujarat', 'Ahmedabad', 'Gujarat', 55000),
        ('Green Valley Foods', 'customer', '36AAGPF1234A1Z5', 'GRVF1234P', '04712345678', 'orders@greenvalley.com', '25, MG Road', 'Kerala', 'Kochi', 'Kerala', 30000),
        # Vendors
        ('Alpha Electronics Supply', 'vendor', '27AABCX1111A1Z5', 'ALPE1111R', '02223456701', 'sales@alphaelec.com', '501, Goregaon Ind Estate', 'Maharashtra', 'Mumbai', 'Maharashtra', 0),
        ('Beta Computers Ltd', 'vendor', '29AAGFY2222A1Z7', 'BETACY2222Q', '08023456701', 'purchase@betacom.com', '88, Electronic City Phase 2', 'Karnataka', 'Bangalore', 'Karnataka', 0),
        ('Delta Office Products', 'vendor', '07AAMEF3333A1Z9', 'DELTA3333K', '01123456701', 'buy@deltaoff.com', '15, Nehru Place', 'Delhi', 'New Delhi', 'Delhi', 0),
        ('Gamma Supplies Inc', 'vendor', '10AADVV4444A1Z3', 'GAMMA4444S', '03323456701', 'order@igmasup.com', '22, Salt Lake', 'West Bengal', 'Kolkata', 'West Bengal', 0),
        ('Omega IT Solutions', 'vendor', '27AABCO5555A1Z5', 'OMEGA5555F', '02223456702', 'info@omegait.com', '301, Andheri Kurla Road', 'Maharashtra', 'Mumbai', 'Maharashtra', 0),
        ('Prime Logistics', 'vendor', '29AAGFL6666A1Z3', 'PRILO6666P', '08023456702', 'accounts@primelog.com', '45, Whitefield', 'Karnataka', 'Bangalore', 'Karnataka', 0),
    ]
    
    party_ids = []
    for party in parties:
        execute_query(c, '''INSERT INTO parties (name, type, gstin, pan, phone, email, address, state, city, place_of_supply, opening_balance, organisation_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (*party, org_id))
        party_ids.append(c.lastrowid if not IS_POSTGRES else None)
    
    if IS_POSTGRES:
        execute_query(c, 'SELECT id FROM parties ORDER BY id DESC LIMIT ?', (len(parties),))
        party_ids = [row['id'] for row in reversed(c.fetchall())]

    conn.commit()
    print(f"Created {len(parties)} parties")
    
    customer_ids = party_ids[:8]
    vendor_ids = party_ids[8:]
    
    # Generate invoices for last 6 months
    inv_num = 1
    pinv_num = 1
    
    for days_ago in range(180, 0, -3):
        date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        # Generate 2-4 invoices per day
        for _ in range(random.randint(2, 4)):
            is_sale = random.random() > 0.3
            party_id = random.choice(customer_ids if is_sale else vendor_ids)
            inv_type = 'sale' if is_sale else 'purchase'
            
            # Get party state
            execute_query(c, 'SELECT state FROM parties WHERE id = ?', (party_id,))
            party_state = c.fetchone()[0 if not IS_POSTGRES else 'state']
            
            # Get org state
            execute_query(c, 'SELECT state FROM organisations WHERE id = ?', (org_id,))
            org_state = c.fetchone()[0 if not IS_POSTGRES else 'state']
            
            is_inter = party_state != org_state
            
            # Generate invoice items
            num_items = random.randint(1, 4)
            selected_items = random.sample(item_ids, num_items)
            
            subtotal = 0
            items_to_add = []
            for item_id in selected_items:
                execute_query(c, 'SELECT rate, gst_rate FROM items WHERE id = ?', (item_id,))
                row = c.fetchone()
                rate = row[0 if not IS_POSTGRES else 'rate']
                gst = row[1 if not IS_POSTGRES else 'gst_rate']
                qty = random.randint(1, 5)
                amount = rate * qty
                subtotal += amount
                items_to_add.append((item_id, qty, rate, gst, amount))

            tax_rate = random.choice([18, 12, 5])
            if is_inter:
                igst = subtotal * tax_rate / 100
                cgst = sgst = 0
            else:
                igst = 0
                cgst = sgst = subtotal * tax_rate / 200
            
            total = subtotal + cgst + sgst + igst
            inv_no = f'INV-{inv_num:04d}' if is_sale else f'PINV-{pinv_num:04d}'
            
            execute_query(c, '''INSERT INTO invoices (invoice_no, party_id, type, date, subtotal, cgst, sgst, igst, total, is_inter_state, status, organisation_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (inv_no, party_id, inv_type, date, subtotal, cgst, sgst, igst, total, 1 if is_inter else 0, 'completed', org_id))
            
            invoice_id = c.lastrowid if not IS_POSTGRES else None
            if IS_POSTGRES:
                execute_query(c, "SELECT id FROM invoices ORDER BY id DESC LIMIT 1")
                invoice_id = c.fetchone()['id']

            for item_id, qty, rate, gst, amount in items_to_add:
                execute_query(c, '''INSERT INTO invoice_items (invoice_id, item_id, quantity, rate, gst_rate, amount)
                             VALUES (?, ?, ?, ?, ?, ?)''',
                          (invoice_id, item_id, qty, rate, gst, amount))
            
            if is_sale:
                inv_num += 1
            else:
                pinv_num += 1
            
            # Add payment receipt for sales
            if is_sale and random.random() > 0.5:
                amount = total
                execute_query(c, '''INSERT INTO transactions (date, type, party_id, amount, mode, reference_no, description, organisation_id)
                             VALUES (?, 'receipt', ?, ?, 'bank', ?, '', ?)''',
                          (date, party_id, amount, f'RCP-{random.randint(10000,99999)}', org_id))
            
            conn.commit()
    
    print(f"Created {inv_num - 1} sales invoices and {pinv_num - 1} purchase invoices")
    
    # Generate transactions
    for days_ago in range(150, 0, -5):
        date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        # Receipts
        if random.random() > 0.4:
            party_id = random.choice(customer_ids)
            amount = random.randint(5000, 100000)
            execute_query(c, '''INSERT INTO transactions (date, type, party_id, amount, mode, reference_no, description, organisation_id)
                         VALUES (?, 'receipt', ?, ?, 'bank', ?, 'Payment received', ?)''',
                      (date, party_id, amount, f'RCP-{random.randint(10000,99999)}', org_id))
        
        # Payments
        if random.random() > 0.4:
            party_id = random.choice(vendor_ids)
            amount = random.randint(3000, 80000)
            execute_query(c, '''INSERT INTO transactions (date, type, party_id, amount, mode, reference_no, description, organisation_id)
                         VALUES (?, 'payment', ?, ?, 'upi', ?, 'Payment made', ?)''',
                      (date, party_id, amount, f'PAY-{random.randint(10000,99999)}', org_id))
        
        conn.commit()
    
    print("Created transactions")
    
    # Generate expenses
    expense_categories = ['Rent', 'Electricity', 'Internet', 'Phone', 'Salary', 'Travel', 'Office Supplies', 'Maintenance']
    for days_ago in range(120, 0, -2):
        date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        if random.random() > 0.3:
            category = random.choice(expense_categories)
            amount = random.randint(1000, 25000)
            execute_query(c, '''INSERT INTO expenses (date, category, description, amount, organisation_id)
                         VALUES (?, ?, ?, ?, ?)''',
                      (date, category, f'{category} expense for the month', amount, org_id))
    
    conn.commit()
    print("Created expenses")
    
    # Generate quotations
    for i in range(15):
        date = (datetime.now() - timedelta(days=random.randint(1, 60))).strftime('%Y-%m-%d')
        valid_date = (datetime.now() + timedelta(days=random.randint(30, 90))).strftime('%Y-%m-%d')
        party_id = random.choice(customer_ids)
        
        execute_query(c, 'SELECT state FROM parties WHERE id = ?', (party_id,))
        party_state = c.fetchone()[0 if not IS_POSTGRES else 'state']
        execute_query(c, 'SELECT state FROM organisations WHERE id = ?', (org_id,))
        org_state = c.fetchone()[0 if not IS_POSTGRES else 'state']
        is_inter = party_state != org_state
        
        num_items = random.randint(2, 4)
        selected_items = random.sample(item_ids, num_items)
        
        subtotal = 0
        items_to_add = []
        for item_id in selected_items:
            execute_query(c, 'SELECT rate, gst_rate FROM items WHERE id = ?', (item_id,))
            row = c.fetchone()
            rate = row[0 if not IS_POSTGRES else 'rate']
            gst = row[1 if not IS_POSTGRES else 'gst_rate']
            qty = random.randint(2, 10)
            amount = rate * qty
            subtotal += amount
            items_to_add.append((item_id, qty, rate, gst, amount))

        tax_rate = 18
        if is_inter:
            igst = subtotal * tax_rate / 100
            cgst = sgst = 0
        else:
            igst = 0
            cgst = sgst = subtotal * tax_rate / 200
        
        total = subtotal + cgst + sgst + igst
        quote_no = f'QT-{i+1:04d}'
        
        execute_query(c, '''INSERT INTO quotations (quote_no, party_id, date, valid_until, subtotal, cgst, sgst, igst, total, is_inter_state, status, organisation_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (quote_no, party_id, date, valid_date, subtotal, cgst, sgst, igst, total, 1 if is_inter else 0, random.choice(['pending', 'accepted', 'rejected']), org_id))
        
        quote_id = c.lastrowid if not IS_POSTGRES else None
        if IS_POSTGRES:
            execute_query(c, "SELECT id FROM quotations ORDER BY id DESC LIMIT 1")
            quote_id = c.fetchone()['id']

        for item_id, qty, rate, gst, amount in items_to_add:
            execute_query(c, '''INSERT INTO quotation_items (quote_id, item_id, quantity, rate, gst_rate, amount)
                         VALUES (?, ?, ?, ?, ?, ?)''',
                      (quote_id, item_id, qty, rate, gst, amount))
    
    conn.commit()
    print("Created quotations")
    
    # Generate purchase orders
    for i in range(10):
        date = (datetime.now() - timedelta(days=random.randint(1, 45))).strftime('%Y-%m-%d')
        delivery_date = (datetime.now() + timedelta(days=random.randint(7, 30))).strftime('%Y-%m-%d')
        party_id = random.choice(vendor_ids)
        
        execute_query(c, 'SELECT state FROM parties WHERE id = ?', (party_id,))
        party_state = c.fetchone()[0 if not IS_POSTGRES else 'state']
        execute_query(c, 'SELECT state FROM organisations WHERE id = ?', (org_id,))
        org_state = c.fetchone()[0 if not IS_POSTGRES else 'state']
        is_inter = party_state != org_state
        
        num_items = random.randint(2, 4)
        selected_items = random.sample(item_ids, num_items)
        
        subtotal = 0
        items_to_add = []
        for item_id in selected_items:
            execute_query(c, 'SELECT rate, gst_rate FROM items WHERE id = ?', (item_id,))
            row = c.fetchone()
            rate = row[0 if not IS_POSTGRES else 'rate']
            gst = row[1 if not IS_POSTGRES else 'gst_rate']
            qty = random.randint(5, 20)
            amount = rate * qty
            subtotal += amount
            items_to_add.append((item_id, qty, rate, gst, amount))

        tax_rate = 18
        if is_inter:
            igst = subtotal * tax_rate / 100
            cgst = sgst = 0
        else:
            igst = 0
            cgst = sgst = subtotal * tax_rate / 200
        
        total = subtotal + cgst + sgst + igst
        po_no = f'PO-{i+1:04d}'
        
        execute_query(c, '''INSERT INTO purchase_orders (po_no, party_id, date, delivery_date, subtotal, cgst, sgst, igst, total, is_inter_state, status, organisation_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (po_no, party_id, date, delivery_date, subtotal, cgst, sgst, igst, total, 1 if is_inter else 0, random.choice(['pending', 'approved', 'completed']), org_id))
        
        po_id = c.lastrowid if not IS_POSTGRES else None
        if IS_POSTGRES:
            execute_query(c, "SELECT id FROM purchase_orders ORDER BY id DESC LIMIT 1")
            po_id = c.fetchone()['id']

        for item_id, qty, rate, gst, amount in items_to_add:
            execute_query(c, '''INSERT INTO purchase_order_items (po_id, item_id, quantity, rate, gst_rate, amount)
                         VALUES (?, ?, ?, ?, ?, ?)''',
                      (po_id, item_id, qty, rate, gst, amount))
    
    conn.commit()
    print("Created purchase orders")
    
    # Update organisation settings with some data
    execute_query(c, '''UPDATE organisations SET 
                 name = 'ProTech Solutions Pvt Ltd',
                 gstin = '27AABCU9876A1Z5',
                 pan = 'TECHS9876Q',
                 phone = '02223456789',
                 email = 'info@protechsolutions.com',
                 address = '501, Business Hub, Andheri East, Mumbai - 400093',
                 state = 'Maharashtra',
                 city = 'Mumbai',
                 gst_type = 'regular',
                 bank_name = 'HDFC Bank',
                 bank_account = '1234567890',
                 bank_ifsc = 'HDFC0001234',
                 bank_branch = 'Andheri East Branch',
                 default_gst_rate = 18,
                 payment_terms = 'Net 30 Days',
                 footer_note = 'Thank you for your business!'
                 WHERE id = ?''', (org_id,))
    
    conn.commit()
    conn.close()
    
    print("\n=== Dummy data seeded successfully! ===")

if __name__ == '__main__':
    seed_dummy_data()
