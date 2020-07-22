from flask import Flask, jsonify,request
from flaskext.mysql import MySQL
from flask_restful import Resource,Api 
import requests
import simplejson as json


app = Flask(__name__)
api = Api(app)

# MySQL configurations
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'northwind'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'

mysql=MySQL(app)

@app.route('/customers/select/<string:customer_id>',methods=['GET'])    #url to sleect customers based on cust_id

def getCustomer(customer_id):
	try:
		#customer_id=request.args['args1']
		customer_id.upper()
		cur = mysql.connect().cursor()
		cur.execute("select * from customers where CustomerID=%s",customer_id)
		result=cur.fetchall()
		if len(result)==0:
			resp=jsonify({"error message":"invalid customer id"})
			resp.status_code=404
		else:
			items={}	#print(result)
			for row in result:
				i=0
				for key in cur.description:

					items[key[0]]=row[i]
					i+=1
			#rows=cur.fetchone()
			resp=jsonify({'Customer_Details':items})
			resp.status_code=200
			cur.close()
	except Exception as e:
		resp=jsonify({'error message':'{}'.format(e)})
		resp.status_code=400
		
	finally:
		
		return resp

 #url to update customer details-cust_id,field to be changed,new value value for the field are passed as args.
@app.route('/customers/update/<string:customer_id>/<string:field>/<string:new_data>',methods=['PUT'])
def updateCustomer(customer_id,field,new_data):
	try:
		customer_id.upper()
		mydb=mysql.connect()
		#cur=mydb.cursor()
		cur1=mydb.cursor()
		r=cur1.execute("select * from customers where CustomerId=%s",customer_id)
		cols=[key[0] for key in cur1.description]
		
		#resp=jsonify({'error message':'db connection error'})
		#resp.status_code=400
		if r==0:
			resp=jsonify({"error message":"invalid customer id"})
			resp.status_code=404
			cur1.close()
		else:
			if field not in cols:
				resp=jsonify({"error message":"invalid field"})
				resp.status_code=404
				cur1.close()
			else:
				cur=mydb.cursor()
				q="""update customers set {}=%s where CustomerID=%s""".format(field)
				cur.execute(q,(new_data,customer_id,))
				mydb.commit()

				resp=jsonify({'{} update'.format(field):'success'})
				resp.status_code=200
				cur.close()
	#return resp
	except Exception as e:
		resp=jsonify({'error message':'{}'.format(e)})
		resp.status_code=400
		
		
	finally:
		return resp

#url to insert new customer
@app.route('/customers/insert/<string:customer_id>/<string:CompanyName>/<string:ContactName>/<string:ContactTitle>/<string:Address>/<string:City>/<string:Region>/<string:PostalCode>/<string:Country>/<string:Phone>/<string:Fax>',methods=['POST'])
def postCustomer(customer_id,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax):
	try:

		customer_id.upper()
		mydb=mysql.connect()
		cur=mydb.cursor()
		q="""insert into customers (CustomerID,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
		cur.execute(q,(customer_id,CompanyName,ContactName,ContactTitle,Address,City,Region,PostalCode,Country,Phone,Fax,))
		mydb.commit()

		resp=jsonify({'{} customer record insertion'.format(customer_id):'success'})
		resp.status_code=200
		cur.close()
		
	except Exception as e:
		resp=jsonify({'error message':'{}'.format(e)})
		resp.status_code=400
	
	finally:
		return resp

#url to get orderhistory of a customer
@app.route('/customers/orderhistory/<string:customer_id>',methods=['GET'])
def get_oder_details(customer_id):
	try:
		customer_id.upper()
		mydb=mysql.connect()
		cur1=mydb.cursor()
		r=cur1.execute("select * from customers where CustomerID=%s",customer_id)
		if r==0:
			resp=jsonify({'error':'invalid customer_id'})
			resp.status_code=400
			cur1.close()
		else:
			cur=mydb.cursor()
			q="""select c.CustomerId,o.OrderID,o.EmployeeID,o.OrderDate,o.RequiredDate,o.ShippedDate,o.ShipVia from customers c, orders o where c.CustomerID=%s and c.CustomerID=o.CustomerID"""
			r=cur.execute(q,(customer_id))
			result=cur.fetchall()
			if r==0:
				resp=jsonify({'message':'no orders by customer {}'.format(customer_id)})
				resp.status_code=400
			else:

				items=[]	#print(result)
				for row in result:
					i=0
					temp={}
					for key in cur.description:

						temp[key[0]]=row[i]
						i+=1
					items.append(temp)
				for i in items:
					del i['CustomerId']
			#rows=cur.fetchone()
				l={'CustomerID':customer_id,'No.of_Orders_in_Total':len(items)}
				l['OrderHistory']=items
				resp=jsonify(l)
				resp.status_code=200
				cur.close()

	except Exception as e:
		resp=jsonify({'error':'{}'.format(e)})
		resp.status_code=400
	finally:
		return resp



@app.route('/products/select/<string:product_id>',methods=['GET'])    #url to select products based on product_id

def getProduct(product_id):
	try:
		
		cur = mysql.connect().cursor()
	#	q="""select * from products where ProductID=%s"""
		cur.execute("""select * from products where ProductID=%s""",int(product_id))
		result=cur.fetchall()
		if len(result)==0:
			resp=jsonify({"error message":"invalid product id"})
			resp.status_code=404
		else:
			items={}	#print(result)
			for row in result:
				i=0
				for key in cur.description:

					items[key[0]]=row[i]
					i+=1
			#rows=cur.fetchone()
			resp=jsonify({'Product_Details':items})
			resp.status_code=200
			cur.close()
	except Exception as e:
		resp=jsonify({'error message':'{}'.format(e)})
		resp.status_code=400
		
	finally:
		return resp

@app.route('/products/insert/<string:product_id>/<string:ProductName>/<string:SupplierID>/<string:CategoryID>/<string:Quantity>/<string:UnitPrice>/<string:UnitsInStock>/<string:UnitsOnOrder>/<string:ReorderLevel>/<string:Discontinued>',methods=['POST'])
def postProduct(product_id,ProductName,SupplierID,CategoryID,Quantity,UnitPrice,UnitsInStock,UnitsOnOrder,ReorderLevel,Discontinued):
	try:

		#customer_id.upper()
		mydb=mysql.connect()
		cur=mydb.cursor()
		q="""insert into products (ProductID,ProductName,SupplierID,CategoryID,QuantityPerUnit,UnitPrice,UnitsInStock,UnitsOnOrder,ReorderLevel,Discontinued) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
		cur.execute(q,(int(product_id),ProductName,int(SupplierID),int(CategoryID),Quantity,float(UnitPrice),int(UnitsInStock),int(UnitsOnOrder),int(ReorderLevel),int(Discontinued),))
		mydb.commit()

		resp=jsonify({'{} product record insertion'.format(product_id):'success'})
		resp.status_code=200
		cur.close()
		
	except Exception as e:
		resp=jsonify({'error message':'{}'.format(e)})
		resp.status_code=400
	
	finally:
		return resp

@app.route('/products/update/<string:product_id>/<string:field>/<string:new_data>',methods=['PUT'])
def updateProduct(product_id,field,new_data):
	try:
		
		mydb=mysql.connect()
		#cur=mydb.cursor()
		cur1=mydb.cursor()
		r=cur1.execute("select * from products where ProductID=%s",int(product_id))
		cols={}
		for key in cur1.description:
			cols[key[0]]=key[1]
			
		
		#resp=jsonify({'error message':'db connection error'})
		#resp.status_code=400
		if r==0:
			resp=jsonify({"error message":"invalid product id"})
			resp.status_code=404
			cur1.close()
		else:
			if field not in cols:
				resp=jsonify({"error message{}".format(cols):"invalid field"})
				resp.status_code=404
				cur1.close()
			else:
				cur=mydb.cursor()
				field_type=cols[field]
				if field_type==246:
					new_data=float(new_data)
				elif field_type==253:
					new_data=str(new_data)
				else:
					new_data=int(new_data)
				
				q="""update products set {}=%s where ProductID=%s""".format(field)
				cur.execute(q,(new_data,int(product_id),))
				mydb.commit()

				resp=jsonify({'{} update'.format(field):'success'})
				resp.status_code=200
				cur.close()
	#return resp
	except Exception as e:
		resp=jsonify({'error message':'{}'.format(e)})
		resp.status_code=400
		
		
	finally:
		return resp

if __name__ == '__main__':
    app.run(debug=True)







