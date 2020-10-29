from flask import Flask, jsonify
from melitk.analytics.connectors.presto import ConnPresto
from datetime import datetime, timedelta
import json
import requests
import re

app = Flask(__name__)

# -------------------------- FUNCIONES PARA GENERALES -------------------------- 
def ejecutarQuery(query):
    config = json.loads(open('config.json').read())
    user = config.get('user')
    password = config.get('pass')
    conn = ConnPresto(user, password)
    conn._establish_connection()
    resultado=conn.execute_response(query)
    return resultado

# -------------------------- FUNCIONES PARA TRACKS - GENERALES -------------------------- 
def queryTracks(payment): 
    #Recibe un payment y devuelve:
    # Si el payment existe: la query para tracks y el date_created del pago
    # Si el payment no existe: 404
    # Si payments falla: "error"
    url = 'http://api.mp.internal.ml.com/v1/payments/'+payment
    args = {'caller.scopes':'admin'}
    res = requests.get(url,params=args)
    if res.status_code==200:
        respuesta = res.json()
        payer_id = respuesta['payer_id']
        date_created = datetime.strptime(respuesta['date_created'],'%Y-%m-%dt%H:%M:%S.%f%z') #Ejemplo: 2020-10-06T10:30:03.000-04:00
        query = """
        SELECT tracks.user_timestamp,
        path,
        device.platform,
        application.version,
        application.business,
        jest(event_data, 'session_id') as session_id,
        event_data
        FROM tracks
        WHERE ds >= '""" + str(date_created.strftime('%Y-%m-%d')) + """'
        AND ds <= '""" + str((date_created + timedelta(days = 1)).strftime('%Y-%m-%d')) + """'
        AND usr.user_id = '""" + str(payer_id) + """'
        ORDER BY tracks.user_timestamp
        """
        resultado = {}
        resultado['query'] = query
        resultado ['date_created'] = date_created
        return(resultado)
    elif(res.status_code == 404):
        return(res.status_code)
    else:
        return('error')

def existeTrackPayment(tracks ,path, paymentID):
    for i in range (len(tracks)):
        if tracks[i]['path']==path:
            event = json.loads(tracks[i]['event_data'])
            #Este if es un parche, ya que el track st_machine_connection_error solo tiene el campo id cuando el event es write
            if path == '/instore/vending/st_machine_connection_error':
                if event['action'] == 'write':
                    if event['id'] == paymentID:
                        return(True)
            elif event['id'] == paymentID:
                return(True)
    return(False)
# -------------------------- FUNCIONES PARA QR/TRACKS -------------------------- 
def detalleCongrats(tracks,payment_id):
    detalle = {'existe':False}
    for i in range (len(tracks)):
        #buscamos que el track sea de result y que no sea de result/abort, porque este último no tiene el campo payment_id
        if re.search('/px_checkout/result',tracks[i]['path']) and not re.search('/abort',tracks[i]['path']) and not re.search('/primary_action',tracks[i]['path']) and not re.search('/continue',tracks[i]['path']):
            event = json.loads(tracks[i]['event_data'])
            if 'payment_id' in event:
                if str(event['payment_id']) == payment_id:
                    detalle['existe'] = True
                    if (event['payment_status']).lower() == 'approved':
                        detalle['color'] = 'verde'
                    elif (event['payment_status']).lower() == 'rejected':
                        detalle['color'] = 'rojo'
                    elif (event['payment_status']).lower() == 'pending':
                        detalle['color'] = 'naranja'
                    else:
                        detalle['color'] = 'desconocido' 
                    return(detalle)
    return(detalle)

def esVendingPost(tracks,paymentID): #SOLO llamar cuando se sepa que hay post_payment
    result = False
    #primero vamos a encontrar la posición del post_payment
    for i in range (len(tracks)):
        if tracks[i]['path']=='/instore/post_payment':
            event = json.loads(tracks[i]['event_data'])
            if event['id'] == paymentID:
                posicionPost = i
                i = len(tracks)
    #teniendo esta posición, recorremos para atrás para encontrar rastros de una vending
    flag = True
    while flag:
        if (tracks[posicionPost]['path']=='/instore/waiting/vending_product_selection') or (tracks[posicionPost]['path']=='/instore/vending/machine_response_state') or (tracks[posicionPost]['path']=='/instore/vending/st_machine_connected'):
            result=True
            flag=False
        elif tracks[posicionPost]['path']=='/px_checkout/review/confirm':
            flag=False
        posicionPost= posicionPost - 1
    return(result)

def tipoDeErrorVending(tracks,paymentID): #SOLO llamar cuando se sepa que hay st_machine_connection_error
    for i in range (len(tracks)):
        if tracks[i]['path']=='/instore/vending/st_machine_connection_error':
            event = json.loads(tracks[i]['event_data'])
            if event['action'] == 'write':
                if event['id'] == paymentID:
                    if re.search('timeout',event['st_machine_connection_error']):
                        return('timeout')
                    elif re.search('Disconnected from',event['st_machine_connection_error']):
                        return('disconnected_from')
                    elif re.search('No se pudo completar la operación',event['st_machine_connection_error']):
                        return('completar_operacion')
                    elif re.search('No se ha podido completar la operación',event['st_machine_connection_error']):
                        return('completar_operacion')
                    elif re.search('The operation couldn’t be completed',event['st_machine_connection_error']):
                        return('completar_operacion')
                    else:
                        return(event['st_machine_connection_error'])

def existePayment_error(tracks,payment): #usar solo cuando se sabe que el payment existe
    #Recibe un payment y devuelve un objeto:
    # (True + posición + error) o (false) para la existencia de un payment_error en la hora del pago (si hay más de uno devuelve el último)
    # Si payments falla: (error)
    url = 'http://api.mp.internal.ml.com/v1/payments/'+payment
    args = {'caller.scopes':'admin'}
    res = requests.get(url,params=args)

    if res.status_code==200:
        respuesta = res.json()
        #date_created contiene la fecha de creación del pago
        date_created = datetime.strptime(respuesta['date_created'],'%Y-%m-%dt%H:%M:%S.%f%z') #Ejemplo: 2020-10-06T10:30:03.000-04:00
        #con eso, buscamos en los tracks algún payment error dentro de ese rango de tiempo (+ - 20 segundos de la creación del pago)
        resultado = {'existe' : False}
        for i in range (len(tracks)):
            if tracks[i]['path']=='/instore/payment_error':
                #user_timestamp contiene la fecha del track
                user_timestamp = datetime.strptime(tracks[i]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z')  
                #En el siguiente if vemos si ese track fue en el rango horario (+ - 20 segundos) de la creación del pago
                if((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) > (date_created - timedelta(seconds = 30)).strftime('%Y-%m-%dt%H:%M:%S')) and ((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) < (date_created + timedelta(seconds = 60)).strftime('%Y-%m-%dt%H:%M:%S')):
                    #guardamos los datos del error y seguimos para devolver la última aparición, que es la que nos importa
                    resultado['existe'] = True
                    resultado['posicion'] = i
                    event = json.loads(tracks[i]['event_data'])
                    resultado['error'] = event
        return resultado
    else:
        return({'existe' : 'error' })

def esVendingPayment_error(tracks,posiError): #SOLO llamar cuando se sepa que hay un payment_error y se tenga su posición
    result = False
    flag = True
    while flag:
        if (tracks[posiError]['path']=='/instore/waiting/vending_product_selection') or (tracks[posiError]['path']=='/instore/vending/machine_response_state') or (tracks[posiError]['path']=='/instore/vending/st_machine_connected'):
            result=True
            flag=False
        elif tracks[posiError]['path']=='/px_checkout/review/confirm':
            flag=False
        posiError= posiError - 1
    return(result)

def detalleCongratsPayment_error(tracks,posiError): #Revisar la congrats después de un payment_error
    detalle = {'existe':False}
    #tomamos la fecha del track original para ir viendo donde paramos de buscar
    user_timestamp_original = datetime.strptime(tracks[posiError]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z') #Ejemplo: 2020-10-06T10:30:03.000-04:00
    while posiError < (len(tracks)):
        #tomamos la fecha del track que estamos viendo para comparar con el original
        user_timestamp_actual = datetime.strptime(tracks[posiError]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z') #Ejemplo: 2020-10-06T10:30:03.000-04:00
        #vamos a ver si estamos dentro de los próximos 15 segundos al payment_error
        if((user_timestamp_actual.strftime('%Y-%m-%dt%H:%M:%S')) <= (user_timestamp_original + timedelta(seconds = 15)).strftime('%Y-%m-%dt%H:%M:%S')):
            #buscamos que el track sea de result y que no sea de result/abort, porque este último no tiene el campo payment_id
            if re.search('/px_checkout/result',tracks[posiError]['path']) and not re.search('/abort',tracks[posiError]['path']) and not re.search('/primary_action',tracks[posiError]['path']) and not re.search('/continue',tracks[posiError]['path']):
                event = json.loads(tracks[posiError]['event_data'])
                detalle['existe'] = True
                if (event['payment_status']).lower() == 'approved':
                    detalle['color'] = 'verde'
                elif (event['payment_status']).lower() == 'rejected':
                    detalle['color'] = 'rojo'
                elif (event['payment_status']).lower() == 'pending':
                    detalle['color'] = 'naranja'
                else:
                    detalle['color'] = 'desconocido'
            posiError = posiError + 1
        else: #si ya nos pasamos de esos 15 segundos, vamos a dejar de buscar
            return (detalle)
    return(detalle)
# -------------------------- FUNCIONES PARA QR/SEARCH -------------------------- 
def querySearch(payment):
    #Recibe un payment y devuelve:
    # Si el payment existe: la query para search
    # Si el payment no existe: 404
    # Si payments falla: "error"
    url = 'http://api.mp.internal.ml.com/v1/payments/'+payment
    args = {'caller.scopes':'admin'}
    res = requests.get(url,params=args)
    if res.status_code==200:
        respuesta = res.json()
        external_reference = respuesta['external_reference']
        date_created = datetime.strptime(respuesta['date_created'],'%Y-%m-%dt%H:%M:%S.%f%z') #Ejemplo: 2020-10-06T10:30:03.000-04:00
        query = """
        SELECT *
        FROM traffic.access_logs_archive
        WHERE ds >= '""" + str(date_created.strftime('%Y-%m-%d')) + """'
        AND ds <= '""" + str((date_created + timedelta(days = 1)).strftime('%Y-%m-%d')) + """'
        AND request_uri like ' """ + external_reference + """'
        AND scope='pubmpapi'
        AND pool_to='search.openplatform-payments-api'
        limit 1000
        """
        return(query)
    elif(res.status_code == 404):
        return(res.status_code)
    else:
        return('error')

def searchPosteriorAlPago (tracks, payment): #SOLO llamar cuando el payment existe y hubo resultados en melidata
    url = 'http://api.mp.internal.ml.com/v1/payments/'+payment
    args = {'caller.scopes':'admin'}
    res = requests.get(url,params=args)
    if res.status_code==200:
        respuesta = res.json()
        date_created = datetime.strptime(respuesta['date_created'],'%Y-%m-%dt%H:%M:%S.%f%z') #Ejemplo: 2020-10-06T10:30:03.000-04:00
        for i in range (len(tracks)):
            time = datetime.strptime(tracks[i]['time'],'%Y-%m-%dt%H:%M:%S.%f%z') #fecha del track
            if time >= date_created:
                return(True)
        return(False)
    else:
        return('error')

# -------------------------- FUNCIONES PARA OFF/TRACKS -------------------------- 
def existeReviewConfirm (tracks, date_created): #Devuelve si existe o no el review/confirm y dice en qué posición está
    resultado = {}
    resultado['existe'] = False
    for i in range (len(tracks)):
        if tracks[i]['path']=='/px_checkout/review/confirm':
            #user_timestamp contiene la fecha del track
            user_timestamp = datetime.strptime(tracks[i]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z')  
            #En el siguiente if vemos si ese track fue en el rango horario (- 50 segundos + 1 minuto) de la creación del pago
            if((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) >= (date_created - timedelta(seconds = 20)).strftime('%Y-%m-%dt%H:%M:%S')) and ((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) <= (date_created + timedelta(seconds = 60)).strftime('%Y-%m-%dt%H:%M:%S')):
                #guardamos los datos del error y seguimos para devolver la última aparición, que es la que nos importa
                resultado['existe'] = True
                resultado['posicion'] = i
                return resultado
    return resultado

def existeReviewDuplicado (tracks, date_created,posiReviewOriginal): #Devuelve si existe o no el review/confirm del duplicado y dice en qué posición está
    resultado = {}
    resultado['existe'] = False
    for i in range (len(tracks)):
        if i != posiReviewOriginal: #Solo vamos a ver el track si no es el del pago original, para no confundir tracks
            if tracks[i]['path']=='/px_checkout/review/confirm':
                #user_timestamp contiene la fecha del track
                user_timestamp = datetime.strptime(tracks[i]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z')  
                #En el siguiente if vemos si ese track fue en el rango horario (- 50 segundos + 1 minuto) de la creación del pago
                if((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) >= (date_created - timedelta(seconds = 20)).strftime('%Y-%m-%dt%H:%M:%S')) and ((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) <= (date_created + timedelta(seconds = 60)).strftime('%Y-%m-%dt%H:%M:%S')):
                    #guardamos los datos del error y seguimos para devolver la última aparición, que es la que nos importa
                    resultado['existe'] = True
                    resultado['posicion'] = i
                    return resultado
    return resultado

def existeCongratsOff(tracks,date_created,posiConfirm,posiNula): #posiNula representa una posición que no hay que tener en cuenta (cuando la congrats ya fue para otro payment). Mandar -1 si no usamos el campo
    #devuelve existe (true o false), posicion, path, payment_status, payment_status_detail
    resultado = {}
    resultado['existe'] = False
    user_timestamp_confirm = datetime.strptime(tracks[posiConfirm]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z')
    while True:
        #user_timestamp contiene la fecha del track
        user_timestamp = datetime.strptime(tracks[posiConfirm]['user_timestamp'],'%Y-%m-%dt%H:%M:%S.%f%z')  
        #En el siguiente if vemos si ese track fue en el rango horario (- 20 segundos, + 60 segundos) de la creación del pago
        if((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) >= (user_timestamp_confirm - timedelta(seconds = 1)).strftime('%Y-%m-%dt%H:%M:%S')) and ((user_timestamp.strftime('%Y-%m-%dt%H:%M:%S')) <= (user_timestamp_confirm + timedelta(seconds = 60)).strftime('%Y-%m-%dt%H:%M:%S')):
            #Si el track está en horario, vemos si es un track de congrats, y si lo es devolvemos los datos que necesitamos
            if re.search('/px_checkout/result',tracks[posiConfirm]['path']) and not re.search('/abort',tracks[posiConfirm]['path']) and not re.search('/primary_action',tracks[posiConfirm]['path']) and not re.search('/continue',tracks[posiConfirm]['path']):
                if posiNula == -1 or posiConfirm != posiNula: #chequeamos que la congrats no haya sido usada en un pago anterior
                    resultado['existe'] = True
                    resultado['posicion'] = posiConfirm
                    resultado['path'] = tracks[posiConfirm]['path']
                    event = json.loads(tracks[posiConfirm]['event_data'])
                    if 'payment_status' in event:
                        resultado['payment_status'] = event ['payment_status']
                    else:
                        resultado['payment_status'] = None
                    if 'payment_status_detail' in event:
                        resultado['payment_status_detail'] = event['payment_status_detail']
                    else:
                        resultado['payment_status_detail'] = None
                    return resultado
            posiConfirm = posiConfirm + 1
            if posiConfirm == len(tracks):
                return resultado
        else: #si no fue en el rango, osea que nos pasamos de horario, devolvemos
            return resultado

# -------------------------- ENDPOINTS -------------------------- 
@app.route('/qr/tracks/<string:payment>',methods=["GET"])
def qr_tracks(payment):
    result = {}
    query = queryTracks(payment)
    if query == 404:
        return {'Error':'Payment no encontrado'},404
    elif query == 'error':
        return {'Error':'Error en api de payments. No se pudo analizar el caso.'},502
    else:
        tracks = ejecutarQuery(query['query'])
        #Primero vemos si hay post_payment
        if existeTrackPayment(tracks,'/instore/post_payment',payment):
            result['tenemosDatos'] = True
            result['paymentError'] = {}
            result['paymentError']['existe'] = False
            #Después vamos a ver que congrats hubo
            congrats = detalleCongrats(tracks,payment)
            result['congrats'] = {}
            result['congrats']['mostrada'] = congrats['existe']
            if congrats['existe']:
                result['congrats']['color'] = congrats['color'] 
            #Después chequeamos si el pago fue a una vending
            result['vending'] = {}
            if esVendingPost(tracks,payment):
                result['vending']['esVending'] = True
            else:
                result['vending']['esVending'] = False
        else: #Si no hay post_payment, vamos a ver si hay payment_error
            payment_error = existePayment_error(tracks,payment) #payment_error tiene ['existe'] true o false, ['posicion'] y ['error']
            if payment_error['existe']:
                result['tenemosDatos'] = True
                result['paymentError'] = {}
                result['paymentError']['existe'] = True
                result['paymentError']['data'] = payment_error['error']
                #Si hubo payment_error, primero vamos a ver si hubo congrats y cuál fue
                congrats = detalleCongratsPayment_error(tracks,payment_error['posicion'])
                result['congrats'] = {}
                result['congrats']['mostrada'] = congrats['existe']
                if congrats['existe']:
                    result['congrats']['color'] = congrats['color'] 
                #Después vamos a ver si fue a una vending
                result['vending'] = {}
                if esVendingPayment_error(tracks,payment_error['posicion']):
                    result['vending']['esVending'] = True
                else:
                    result['vending']['esVending'] = False
            elif not payment_error['existe']:
                #Si no hubo payment_error, vamos a tomar como que no tenemos ningún dato del pago
                result['tenemosDatos'] = False
                return result,200
            else: #si falló la búsqueda de payment_error por la api de payments, devolvemos esa info
                return {'Error':'Error en api de payments. Caso sin post_payment.'},502        
        #si vimos que fue un pago a una vending, vamos a ver si hubo errores con la máquina
        if result['vending']['esVending']:
            if existeTrackPayment(tracks,'/instore/vending/st_machine_connection_error',payment):
                #Si hubo errores marcamos cuál fue el error que se identificó
                result['vending']['huboError'] = True
                result['vending']['tipoError'] = tipoDeErrorVending(tracks,payment)
            else:
                result['vending']['huboError'] = False
                #Si no hubo error, vamos a ver si terminó la operación
                if existeTrackPayment(tracks,'/instore/vending/machine_response_final_result',payment):
                    result['vending']['operacionFInalizada'] = True
                else:
                    result['vending']['operacionFInalizada'] = False
        return result,200

@app.route('/qr/search/<string:payment>',methods=["GET"])
def qr_search(payment):
    result = {}
    query = querySearch(payment)
    if query == 404:
        return {'Error':'Payment no encontrado'},404
    elif query == 'error':
        return {'Error':'Error en api de payments. No se pudo analizar el caso.'},502
    else:
        searchs = ejecutarQuery(query)
        #vemos si hubo busquedas por parte del integrador
        if searchs == []: #si no las hubo
            result['huboBusqueda'] = False
        else:
            result['huboBusqueda'] = True
            #Si la hubo, vamos a ver si fueron anteriores o posteriores al pago
            posteriorAlPago = searchPosteriorAlPago(searchs,payment)
            if posteriorAlPago == 'error':
                return {'Error':'Error en api de payments. No se pudo analizar el caso.'},502
            elif posteriorAlPago == True:
                result['busquedaPosterior'] = True
            else:
                result['busquedaPosterior'] = False
        return result,200

@app.route('/off/tracks/<string:payment>',methods = ["GET"])
def off_tracks(payment):
    result = {}
    query = queryTracks(payment)
    if query == 404:
        return {'Error':'Payment no encontrado'},404
    elif query == 'error':
        return {'Error':'Error en api de payments. No se pudo analizar el caso.'},502
    else:
        tracks = ejecutarQuery(query['query']) #guardamos el resultado de la query
        date_created = query['date_created'] #guardamos la fecha de creación del pago
        #Primero vamos a ver si hay review/confirm
        review = existeReviewConfirm(tracks,date_created)
        if review['existe']:
            result['tenemosDatos'] = True
            #si hubo review/confirm, vamos a ver la congrats
            congrats = existeCongratsOff(tracks,date_created,review['posicion'],-1)
            if congrats['existe']:
                result['congrats'] = {}
                result['congrats']['mostrada'] = True
                result['congrats']['path'] = congrats['path']
                result['congrats']['status'] = congrats['payment_status']
                result['congrats']['status_details'] = congrats['payment_status_detail']
            else:
                result['congrats'] = {}
                result['congrats']['mostrada'] = False
        else:
            result['tenemosDatos'] = False
        return result

@app.route('/off/duplicados/<string:payment_original>/<string:payment_duplicado>',methods = ["GET"])
def off_duplicados(payment_original,payment_duplicado):
    result = {}
    if payment_original > payment_duplicado: #Si el que entra como duplicado es el original, los cambiamos
        aux = payment_original
        payment_original = payment_duplicado
        payment_duplicado = aux
    query = queryTracks(payment_original)
    if query == 404:
        return {'Error':'Payment no encontrado'},404
    elif query == 'error':
        return {'Error':'Error en api de payments. No se pudo analizar el caso.'},502
    else:
        tracks = ejecutarQuery(query['query']) #guardamos el resultado de la query
        date_created_original = query['date_created'] #guardamos la fecha de creación del pago original
        query = queryTracks(payment_duplicado)
        date_created_duplicado = query['date_created'] #guardamos la fecha de creación del pago duplicado
        #primero vamos a ver si hay review confirm para el pago original
        review_original = existeReviewConfirm(tracks,date_created_original)
        if review_original['existe']: #Si hay para el original, vamos a ver si hay para el duplicado
            result['tenemosDatos'] = True
            review_duplicado = existeReviewDuplicado(tracks,date_created_duplicado,review_original['posicion'])
            if review_duplicado['existe']:
                result['duplicados'] = False
                #Si no son duplicados, vamos a ver las congrats de cada uno
                payments = [payment_original,payment_duplicado]
                creaciones = [date_created_original,date_created_duplicado]
                confirms = [review_original['posicion'], review_duplicado['posicion']]
                result['payment'] = []
                for i in range (len(payments)):
                    datos = {}
                    datos['id'] = payments[i]
                    if i==0:
                        congrats = existeCongratsOff(tracks,creaciones[i],confirms[i],-1)
                    else:
                        #Al hacer este llamado, congrats todavía tiene el valor de la pasada anterior. Entonces mandamos la posición de la congrats anterior para que no sea tomada en cuenta
                        congrats = existeCongratsOff(tracks,creaciones[i],confirms[i],congrats['posicion']) 
                    datos['congrats'] = {}
                    datos['congrats']['mostrada'] = congrats['existe']
                    if congrats['existe']:
                        datos['congrats']['path'] = congrats['path']
                        datos['congrats']['status'] = congrats['payment_status']
                        datos['congrats']['status_details'] = congrats['payment_status_detail']
                    result['payment'].append(datos)
            else:
                result['duplicados'] = True
        else:
            #Si no hay datos para el original, vamos a ver si hay datos para el duplicado
            review_duplicado = existeReviewConfirm(tracks,date_created_duplicado)
            if review_duplicado['existe']: #Si hubo datos para el duplicado pero no para el original, entendemos que son duplicados
                result['tenemosDatos'] = True
                result['duplicados'] = True
            else: #Si no hubo datos para ninguno de los dos, entonces no hay datos
                result['tenemosDatos'] = False
        return result

if __name__ == "__main__":
    app.run(debug=True)