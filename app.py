import os
from flask import Flask, render_template, request, redirect, url_for, session, flash
from pymongo import MongoClient, errors
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from dotenv import load_dotenv

# Load .env
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
SECRET_KEY = os.getenv("SECRET_KEY", "devsecret")

app = Flask(__name__)
app.secret_key = SECRET_KEY

# Connect to MongoDB
try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.server_info()
except Exception as e:
    print("WARNING: could not connect to MongoDB:", e)
    client = None

# Config
ADMIN_DB = "managementdb"
DB_NAMES = ['Nesco1','Nesco2','Nesco3','Desco1','Desco2','Desco3','PBS1','PBS2','PBS3']

def admin_coll():
    return client[ADMIN_DB]['admin'] if client is not None else None

def company_coll():
    return client[ADMIN_DB]['company'] if client is not None else None

def choose_db(location: str, user_id: int = None) -> str:
    loc = (location or '').strip().lower()
    if loc in ('rajshahi', 'nesco'):
        prefix = 'Nesco'
    elif loc in ('dhaka', 'desco'):
        prefix = 'Desco'
    else:
        prefix = 'PBS'

    if user_id is None:
        # Default: pick the DB with most users? Or suffix 1
        suffix = '1'
    elif 1 <= user_id <= 100:
        suffix = '1'
    elif 101 <= user_id <= 200:
        suffix = '2'
    elif 201 <= user_id <= 300:
        suffix = '3'
    else:
        suffix = 'default'
    return f"{prefix}{suffix}"


def get_db_for_location(location, user_id=1):
    if client is None:
        return None
    db_name = choose_db(location, user_id)
    try:
        return client[db_name]
    except Exception:
        return None

def get_collections(db):
    if db is None:
        return {}
    return {
        'Agent': db['Agent'],
        'Prepaid': db['Prepaid'],
        'Postpaid': db['Postpaid'],
        'Meter_inf': db['Meter_inf'],
        'Bill': db['Bill']
    }

# Helpers
def login_required(f):
    @wraps(f)
    def wrapper(*a, **kw):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*a, **kw)
    return wrapper

def role_required(roles):
    def dec(f):
        @wraps(f)
        def wrapper(*a, **kw):
            u = session.get('user')
            if not u or u.get('user_type') not in roles:
                flash("Access denied", "error")
                return redirect(url_for('dashboard'))
            return f(*a, **kw)
        return wrapper
    return dec

@app.context_processor
def inject_user():
    return dict(user=session.get('user'))

@app.route('/')
def index():
    return redirect(url_for('login'))

##login system
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        password = request.form.get('password')
        location = request.form.get('location', 'other').lower()

        # --- Validate ID ---
        try:
            user_id = int(user_id)
        except:
            flash("Invalid ID format", "error")
            return redirect(url_for('login'))

        # ----------------------------------------------------
        # 1️⃣ TRY LOGIN FROM CENTRAL DB (ADMIN + COMPANY)
        # ----------------------------------------------------
        admin_col_obj = admin_coll()
        admin_user = None
        if admin_col_obj is not None:
            admin_user = admin_col_obj.find_one({'id': user_id})

        if admin_user and check_password_hash(admin_user.get('password', ''), password):
            session['user'] = {
                'id': admin_user['id'],
                'user_type': 'admin',
                'location': 'admin'
            }
            flash("Logged in as Admin", "success")
            return redirect(url_for('dashboard'))

        company_col_obj = company_coll()
        company_user = None
        if company_col_obj is not None:
            company_user = company_col_obj.find_one({'id': user_id})

        if company_user and check_password_hash(company_user.get('password', ''), password):
            session['user'] = {
                'id': company_user['id'],
                'user_type': 'company',
                'location': company_user.get('location', 'other')
            }
            flash("Logged in as Company", "success")
            return redirect(url_for('dashboard'))

        # ----------------------------------------------------
        # 2️⃣ TRY LOGIN FROM DISTRIBUTED COMPANY DATABASES
        # ----------------------------------------------------
        # use location + id to choose DB:
        db = get_db_for_location(location, user_id)
        cols = get_collections(db)

        lookup_order = [
            ('Agent', 'agent'),
            ('Prepaid', 'customer_prepaid'),
            ('Postpaid', 'customer_postpaid')
        ]

        for col_name, user_type in lookup_order:
            col = cols.get(col_name)
            if col is None:
                continue

            user = col.find_one({'id': user_id})
            if user and check_password_hash(user.get('password', ''), password):
                session['user'] = {
                    'id': user_id,
                    'user_type': user_type,
                    'location': location,
                    'db': db.name if db is not None else None
                }
                flash(f"Logged in as {user_type}", "success")
                return redirect(url_for('dashboard'))

        # ----------------------------------------------------
        # 3️⃣ NO MATCH FOUND
        # ----------------------------------------------------
        flash("Invalid credentials or user not found", "error")
        return redirect(url_for('login'))

    # GET → show login page
    return render_template('login.html')

##
@app.route('/logout')
def logout():
    session.clear()
    flash("Logged out", "success")
    return redirect(url_for('login'))

@app.route('/dashboard')
@login_required
def dashboard():
    u = session['user']
    role = u['user_type']
    if role == 'admin':
        admin_coll_obj = admin_coll()
        company_coll_obj = company_coll()
        admins = list(admin_coll_obj.find({})) if admin_coll_obj is not None else []
        companies = list(company_coll_obj.find({})) if company_coll_obj is not None else []
        summary = []
        for dbn in DB_NAMES:
            try:
                db = client[dbn] if client is not None else None
                cols = get_collections(db)
                users_count = 0
                for k in ('Agent','Prepaid','Postpaid'):
                    col = cols.get(k)
                    if col is not None:
                        try:
                            users_count += col.count_documents({})
                        except Exception:
                            users_count += 0
                bills_count = 0
                bill_col = cols.get('Bill')
                if bill_col is not None:
                    try:
                        bills_count = bill_col.count_documents({})
                    except Exception:
                        bills_count = 0
            except Exception:
                users_count = 0
                bills_count = 0
            summary.append({'db': dbn, 'users': users_count, 'bills': bills_count})
        return render_template('dashboard_admin.html', admin_users=admins+companies, summary=summary)

    if role == 'company':
        company_coll_obj = company_coll()

        # get company document safely
        comp = company_coll_obj.find_one({'id': u['id']}) if company_coll_obj is not None else None

        location = comp.get('location', 'other').lower() if comp else 'other'

        # Determine DB prefix
        if location in ('rajshahi', 'nesco'):
            prefix = 'Nesco'
        elif location in ('dhaka', 'desco'):
            prefix = 'Desco'
        else:
            prefix = 'PBS'

        # Load all DBs for this company
        db_list = []
        if client is not None:
            for suf in ('1','2','3'):
                try:
                    db_list.append(client[f"{prefix}{suf}"])
                except Exception:
                    # skip missing DBs
                    continue

        agents = []
        prepaid = []
        postpaid = []
        bills = []

        for db in db_list:
            cols = get_collections(db)

            agent_col = cols.get('Agent')
            if agent_col is not None:
                try:
                    agents.extend(list(agent_col.find({})))
                except Exception:
                    pass

            pp_col = cols.get('Prepaid')
            if pp_col is not None:
                try:
                    prepaid.extend(list(pp_col.find({})))
                except Exception:
                    pass

            po_col = cols.get('Postpaid')
            if po_col is not None:
                try:
                    postpaid.extend(list(po_col.find({})))
                except Exception:
                    pass

            bill_col = cols.get('Bill')
            if bill_col is not None:
                try:
                    bills.extend(list(bill_col.find({})))
                except Exception:
                    pass

        return render_template(
            'dashboard_company.html',
            company=comp,
            agents=agents,
            prepaid=prepaid,
            postpaid=postpaid,
            bills=bills
        )

    if role == 'agent':
        # Derive location prefix same as company
        location = u.get('location', 'other').lower()

        if location in ('rajshahi', 'nesco'):
            prefix = 'Nesco'
        elif location in ('dhaka', 'desco'):
            prefix = 'Desco'
        else:
            prefix = 'PBS'

        # Load distributed DBs
        db_list = []
        if client is not None:
            for suf in ('1', '2', '3'):
                try:
                    db_list.append(client[f"{prefix}{suf}"])
                except Exception:
                    continue

        # Collect bills from all DB shards
        bills = []
        for db in db_list:
            cols = get_collections(db)
            bill_col = cols.get('Bill')
            if bill_col is not None:
                try:
                    bills.extend(list(bill_col.find({})))  # <-- NO FILTER
                except Exception:
                    pass

        return render_template('dashboard_agent.html', bills=bills)

    # Customer role logic
    db = get_db_for_location(u.get('location', 'other'), u['id']) if client is not None else None
    cols = get_collections(db)
    profile = None
    pp_col = cols.get('Prepaid')
    if pp_col is not None:
        profile = pp_col.find_one({'id': u['id']})

    if profile is None:
        po_col = cols.get('Postpaid')
        if po_col is not None:
            profile = po_col.find_one({'id': u['id']})

    bill_col = cols.get('Bill')
    bills = list(bill_col.find({'id': u['id']})) if bill_col is not None else []

    return render_template('dashboard_customer.html', profile=profile, bills=bills)

#####
@app.route('/admin/create_company', methods=['GET','POST'])
@login_required
@role_required(['admin'])
def admin_create_company():
    if request.method == 'POST':
        data = request.form
        try:
            new_id = int(data['id'])
        except Exception:
            flash("Invalid company id", "error")
            return redirect(url_for('dashboard'))
        doc = {'id': new_id, 'user_type': 'company', 'location': data.get('location','other'), 'password': generate_password_hash(data['password'])}
        company_coll_obj = company_coll()
        if company_coll_obj is not None:
            company_coll_obj.update_one({'id': new_id}, {'$set': doc}, upsert=True)
            flash("Company user created", "success")
        else:
            flash("Database connection error", "error")
        return redirect(url_for('dashboard'))
    return render_template('admin_create_company.html')

@app.route('/admin/edit_company/<int:company_id>', methods=['GET','POST'])
@login_required
@role_required(['admin'])
def admin_edit_company(company_id):
    company_coll_obj = company_coll()
    comp = company_coll_obj.find_one({'id': company_id}) if company_coll_obj is not None else None
    if comp is None:
        flash("Company not found", "error")
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        data = request.form
        update = {}
        if data.get('password'):
            update['password'] = generate_password_hash(data['password'])
        if data.get('location'):
            update['location'] = data['location']
        if company_coll_obj is not None:
            company_coll_obj.update_one({'id': company_id}, {'$set': update})
            flash("Company updated", "success")
        else:
            flash("Database connection error", "error")
        return redirect(url_for('dashboard'))
    return render_template('admin_edit_company.html', comp=comp)

@app.route('/company/create_user', methods=['GET','POST'])
@login_required
@role_required(['company'])
def company_create_user():
    company_user = session['user']  # logged-in company
    company_location = company_user.get('location', 'other')

    if request.method == 'POST':
        data = request.form
        try:
            new_id = int(data['id'])
        except Exception:
            flash("Invalid ID", "error")
            return redirect(url_for('dashboard'))
        name = data.get('name')
        role = data.get('user_type')

        db = get_db_for_location(company_location, new_id)
        if db is None:
            flash("Database connection error", "error")
            return redirect(url_for('dashboard'))

        cols = get_collections(db)

        # Base document
        doc = {
            'id': new_id,
            'name': name,
            'location': company_location,
            'password': generate_password_hash(data['password']),
            'user_type': role
        }

        if role == 'agent':
            agent_col = cols.get('Agent')
            if agent_col is not None:
                agent_col.update_one({'id': new_id}, {'$set': doc}, upsert=True)
            else:
                flash("Agent collection not available", "error")
                return redirect(url_for('dashboard'))
        else:
            # Customer
            customer_type = data.get('customer_type', 'prepaid')
            doc['customer_type'] = customer_type

            # Generate meter number (safer lower-case handling)
            prefix_map = {'dhaka': 'DH', 'rajshahi': 'RH', 'other': 'AL'}
            prefix = prefix_map.get(company_location.lower(), 'AL')
            meter_col = cols.get('Meter_inf')
            last_number = 0
            if meter_col is not None:
                try:
                    last_meter = meter_col.find_one(sort=[("meter_no", -1)])
                    if last_meter and 'meter_no' in last_meter:
                        try:
                            last_number = int(last_meter['meter_no'].split('_')[-1])
                        except Exception:
                            last_number = 0
                except Exception:
                    last_number = 0
            meter_no = f"{prefix}_{last_number + 1:06d}"
            doc['meter_no'] = meter_no

            # Create Meter_info document
            meter_doc = {
                'meter_no': meter_no,
                'location': company_location,
                'unit_usage': float(data.get('unit_usage', 0) or 0)
            }
            if meter_col is not None:
                try:
                    meter_col.update_one({'meter_no': meter_no}, {'$set': meter_doc}, upsert=True)
                except Exception:
                    pass

            # Handle prepaid/postpaid fields
            if customer_type == 'prepaid':
                doc['balance'] = float(data.get('balance') or 0)
                doc['recharge_date'] = data.get('recharge_date')
                pp_col = cols.get('Prepaid')
                if pp_col is not None:
                    pp_col.update_one({'id': new_id}, {'$set': doc}, upsert=True)
                else:
                    flash("Prepaid collection not available", "error")
                    return redirect(url_for('dashboard'))
            else:  # postpaid
                doc['due_date'] = data.get('due_date')
                po_col = cols.get('Postpaid')
                if po_col is not None:
                    po_col.update_one({'id': new_id}, {'$set': doc}, upsert=True)
                else:
                    flash("Postpaid collection not available", "error")
                    return redirect(url_for('dashboard'))

        flash("Created successfully", "success")
        return redirect(url_for('dashboard'))

    return render_template('company_create_user.html')

#update user
@app.route('/company/company_update_user/<user_type>/<int:user_id>', methods=['GET', 'POST'])
@login_required
@role_required(['company'])
def company_update_user(user_type, user_id):
    company_user = session['user']
    company_location = company_user.get('location', 'other')
    db = get_db_for_location(company_location, user_id)
    if db is None:
        flash("Database connection error", "error")
        return redirect(url_for('dashboard'))

    cols = get_collections(db)

    # Determine collection
    col_map = {'agent': 'Agent', 'prepaid': 'Prepaid', 'postpaid': 'Postpaid'}
    collection = col_map.get(user_type)
    if cols.get(collection) is None:
        flash("Collection not found", "error")
        return redirect(url_for('dashboard'))

    user = cols[collection].find_one({'id': user_id}) if cols.get(collection) is not None else None
    if user is None:
        flash("User not found", "error")
        return redirect(url_for('dashboard'))

    if request.method == 'POST':
        data = request.form
        update_doc = {}
        if data.get('name'):
            update_doc['name'] = data['name']
        if data.get('password'):
            update_doc['password'] = generate_password_hash(data['password'])
        if update_doc:
            cols[collection].update_one({'id': user_id}, {'$set': update_doc})
            flash("User updated successfully", "success")
        return redirect(url_for('dashboard'))

    return render_template('company_update_user.html', user=user, user_type=user_type)


@app.route('/company/edit_user/<int:user_id>', methods=['GET','POST'])
@login_required
@role_required(['company'])
def company_edit_user(user_id):
    u = session['user']
    db = get_db_for_location(u.get('location','other'), user_id)
    if db is None:
        flash("Database connection error", "error")
        return redirect(url_for('dashboard'))
        
    cols = get_collections(db)
    user_doc = None
    agent_col = cols.get('Agent')
    if agent_col is not None:
        user_doc = agent_col.find_one({'id': user_id})
    if user_doc is None:
        pp_col = cols.get('Prepaid')
        if pp_col is not None:
            user_doc = pp_col.find_one({'id': user_id})
    if user_doc is None:
        po_col = cols.get('Postpaid')
        if po_col is not None:
            user_doc = po_col.find_one({'id': user_id})
        
    if user_doc is None:
        flash("User not found", "error")
        return redirect(url_for('dashboard'))
    if request.method == 'POST':
        data = request.form
        update = {}
        if data.get('password'):
            update['password'] = generate_password_hash(data['password'])
        if 'balance' in data and data.get('balance')!='':
            try:
                update['balance'] = float(data.get('balance') or 0)
            except Exception:
                update['balance'] = 0
        if 'bill_amount' in data and data.get('bill_amount')!='':
            try:
                update['bill_amount'] = float(data.get('bill_amount') or 0)
            except Exception:
                update['bill_amount'] = 0
        if user_doc.get('user_type') == 'agent':
            agent_col = cols.get('Agent')
            if agent_col is not None:
                agent_col.update_one({'id': user_id}, {'$set': update})
        elif user_doc.get('customer_type') == 'prepaid':
            pp_col = cols.get('Prepaid')
            if pp_col is not None:
                pp_col.update_one({'id': user_id}, {'$set': update})
        else:
            po_col = cols.get('Postpaid')
            if po_col is not None:
                po_col.update_one({'id': user_id}, {'$set': update})
        flash("Updated", "success")
        return redirect(url_for('dashboard'))
    return render_template('company_edit_user.html', target=user_doc)

#bills for postpaid
@app.route('/company/postpaid_users')
@login_required
@role_required(['company'])
def company_postpaid_users():
    u = session['user']
    location = u.get('location', 'other').lower()

    # Determine DB prefix
    if location in ('rajshahi', 'nesco'):
        prefix = 'Nesco'
    elif location in ('dhaka', 'desco'):
        prefix = 'Desco'
    else:
        prefix = 'PBS'

    # Collect from all shards
    db_list = []
    if client is not None:
        for suf in ('1','2','3'):
            try:
                db_list.append(client[f"{prefix}{suf}"])
            except Exception:
                continue

    final_users = []
    for db in db_list:
        cols = get_collections(db)
        postpaid_col = cols.get('Postpaid')
        bill_col = cols.get('Bill')
        users = []
        if postpaid_col is not None:
            try:
                users = list(postpaid_col.find({}))
            except Exception:
                users = []
        for user in users:
            uid = user.get('id')
            unpaid = None
            if bill_col is not None:
                try:
                    unpaid = bill_col.find_one({"id": uid, "status": "unpaid"})
                except Exception:
                    unpaid = None

            outstanding = 0
            fine = 0

            if unpaid:
                outstanding = unpaid.get("amount", 0)
                fine = 50  # auto rule

            final_users.append({
                "id": uid,
                "name": user.get("name"),
                "meter_no": user.get("meter_no"),
                "location": user.get("location"),
                "outstanding": outstanding,
                "fine": fine
            })

    return render_template("company_postpaid_users.html", users=final_users)

#the bills
@app.route('/company/bill_user/<int:user_id>', methods=['GET', 'POST'])
@login_required
@role_required(['company'])
def company_bill_user(user_id):
    u = session['user']
    location = u.get('location', 'other')

    db = get_db_for_location(location, user_id)
    cols = get_collections(db)

    po_col = cols.get('Postpaid')
    if po_col is None:
        flash("Postpaid collection not available", "danger")
        return redirect(url_for('company_postpaid_users'))

    # Load postpaid user
    user = po_col.find_one({"id": user_id}) if po_col is not None else None
    if not user:
        flash("User not found", "danger")
        return redirect(url_for('company_postpaid_users'))

    bill_col = cols.get('Bill')

    # ---- Get latest bill (only 1) ----
    old_bill = None
    if bill_col is not None:
        try:
            old_bill = bill_col.find_one(
                {"id": user_id},
                sort=[("_id", -1)]
            )
        except Exception:
            old_bill = None

    # Default values
    old_amount = 0
    fine = 0

    # ---- If there is an old bill ----
    if old_bill:
        old_amount = float(old_bill.get("amount", 0) or 0)

        # fine only if old bill status is unpaid
        if old_bill.get("status") == "unpaid":
            fine = 50
        else:
            fine = 0  # paid or replaced

    # ---- Handle POST request ----
    if request.method == "POST":
        try:
            new_amount = float(request.form.get("amount") or 0)
        except Exception:
            new_amount = 0
        due_date = request.form.get("due_date")

        total = new_amount + old_amount + fine

        new_bill = {
            "id": user_id,
            "location": location,
            "amount": total,
            "due_date": due_date,
            "status": "unpaid",
            "fine": fine,
            "base_amount": new_amount,
            "previous_due": old_amount
        }

        if bill_col is not None:
            try:
                bill_col.insert_one(new_bill)
            except Exception:
                flash("Failed to create bill", "error")
                return redirect(url_for('company_postpaid_users'))

            # mark old bill as replaced (only if old exists and only if unpaid)
            if old_bill and old_bill.get("status") == "unpaid":
                try:
                    bill_col.update_one(
                        {"_id": old_bill["_id"]},
                        {"$set": {"status": "replaced"}}
                    )
                except Exception:
                    pass

        flash("Bill generated successfully", "success")
        return redirect(url_for('company_postpaid_users'))

    return render_template(
        "company_bill_user.html",
        user=user,
        old_amount=old_amount,
        fine=fine
    )

#agent

@app.route('/agent/pay', methods=['POST'])
@login_required
@role_required(['agent'])
def agent_pay():
    u = session['user']
    try:
        user_id = int(request.form['user_id'])
    except Exception:
        flash("Invalid user id", "error")
        return redirect(url_for('dashboard'))
    try:
        amount = float(request.form.get('amount') or 0)
    except Exception:
        amount = 0
    db = get_db_for_location(u.get('location','other'), user_id)
    if db is None:
        flash("Database connection error", "error")
        return redirect(url_for('dashboard'))
        
    cols = get_collections(db)
    pp_col = cols.get('Prepaid')
    bill_col = cols.get('Bill')

    prepaid = pp_col.find_one({'id': user_id}) if pp_col is not None else None
    if prepaid:
        new_balance = (prepaid.get('balance') or 0) + amount
        try:
            pp_col.update_one({'id': user_id}, {'$set': {'balance': new_balance}})
            flash("Recharged prepaid account", "success")
        except Exception:
            flash("Failed to update balance", "error")
        return redirect(url_for('dashboard'))

    unpaid = bill_col.find_one({'id': user_id, 'status': 'unpaid'}) if bill_col is not None else None
    if unpaid:
        try:
            bill_col.update_one({'_id': unpaid['_id']}, {'$set': {'status': 'paid'}})
            flash("Marked a bill as paid", "success")
        except Exception:
            flash("Failed to mark bill paid", "error")
        return redirect(url_for('dashboard'))
    flash("No target found to pay", "error")
    return redirect(url_for('dashboard'))

@app.route('/admin/delete_company/<int:company_id>', methods=['POST'])
@login_required
@role_required(['admin'])
def admin_delete_company(company_id):
    company_coll_obj = company_coll()
    if company_coll_obj is not None:
        try:
            company_coll_obj.delete_one({'id': company_id})
            flash("Company removed", "success")
        except Exception:
            flash("Failed to remove company", "error")
    else:
        flash("Database connection error", "error")
    return redirect(url_for('dashboard'))

@app.route('/company/delete_user/<int:user_id>', methods=['POST'])
@login_required
@role_required(['company'])
def company_delete_user(user_id):
    u = session['user']
    db = get_db_for_location(u.get('location','other'), user_id)
    if db is not None:
        cols = get_collections(db)
        agent_col = cols.get('Agent')
        if agent_col is not None:
            try:
                agent_col.delete_one({'id': user_id})
            except Exception:
                pass
        pp_col = cols.get('Prepaid')
        if pp_col is not None:
            try:
                pp_col.delete_one({'id': user_id})
            except Exception:
                pass
        po_col = cols.get('Postpaid')
        if po_col is not None:
            try:
                po_col.delete_one({'id': user_id})
            except Exception:
                pass
        flash("User deleted", "success")
    else:
        flash("Database connection error", "error")
    return redirect(url_for('dashboard'))

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
