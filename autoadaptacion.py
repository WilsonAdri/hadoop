import requests
import time
import subprocess

YARN_API_URL = "http://localhost:8088/ws/v1/cluster/metrics"

def aplicar_restriccion(memoria_actual):
    print(f"\n>>> [AUTOADAPTACIÓN] 🚨 ALERTA: Memoria crítica detectada ({memoria_actual:.1f}%).")
    print(">>> [AUTOADAPTACIÓN] ⚙️ FASE ANALYZE/PLAN: Generando regla para limitar capacidad al 50%...")
    
    # ¡CORRECCIÓN!: Creamos una cola 'default' (50%) y una cola 'reserva' (50%) para que sumen 100%
    xml_restringido = """<?xml version="1.0"?>
    <configuration>
      <property><name>yarn.scheduler.capacity.root.queues</name><value>default,reserva</value></property>
      <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>50</value></property>
      <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>50</value></property>
      <property><name>yarn.scheduler.capacity.root.reserva.capacity</name><value>50</value></property>
      <property><name>yarn.scheduler.capacity.root.reserva.maximum-capacity</name><value>50</value></property>
    </configuration>"""
    
    with open("regla_temporal.xml", "w") as f:
        f.write(xml_restringido)

    print(">>> [AUTOADAPTACIÓN] 🛠️ FASE EXECUTE: Inyectando regla y refrescando YARN...")
    subprocess.run("docker cp regla_temporal.xml namenode:/usr/local/hadoop/etc/hadoop/capacity-scheduler.xml", shell=True)
    # Quitamos el DEVNULL para que, si hay un error, lo podamos ver en pantalla
    subprocess.run("docker exec namenode yarn rmadmin -refreshQueues", shell=True)
    print(">>> [AUTOADAPTACIÓN] ✅ ÉXITO: Clúster limitado matemáticamente.\n")

def restaurar_capacidad():
    print("\n>>> [AUTOADAPTACIÓN] 🟢 INFO: El clúster está libre. Restaurando capacidad al 100%...")
    
    # Volvemos a dejar solo la cola 'default' al 100%
    xml_normal = """<?xml version="1.0"?>
    <configuration>
      <property><name>yarn.scheduler.capacity.root.queues</name><value>default</value></property>
      <property><name>yarn.scheduler.capacity.root.default.capacity</name><value>100</value></property>
      <property><name>yarn.scheduler.capacity.root.default.maximum-capacity</name><value>100</value></property>
    </configuration>"""
    
    with open("regla_temporal.xml", "w") as f:
        f.write(xml_normal)

    subprocess.run("docker cp regla_temporal.xml namenode:/usr/local/hadoop/etc/hadoop/capacity-scheduler.xml", shell=True)
    subprocess.run("docker exec namenode yarn rmadmin -refreshQueues", shell=True)
    print(">>> [AUTOADAPTACIÓN] ✅ ÉXITO: Paralelismo restaurado a la normalidad.\n")

def monitorear_cluster():
    print("Iniciando Bucle Autoadaptativo MAPE-K Completo...")
    print("Esperando cargas de trabajo...\n")
    
    estado_restringido = False 
    
    while True:
        try:
            respuesta = requests.get(YARN_API_URL)
            datos = respuesta.json()['clusterMetrics']
            
            memoria_total = datos['availableMB'] + datos['allocatedMB']
            porcentaje_uso = (datos['allocatedMB'] / memoria_total) * 100 if memoria_total > 0 else 0
                
            print(f"[MONITOR] Memoria en uso: {porcentaje_uso:.1f}% | Contenedores: {datos['containersAllocated']}")
            
            # --- TOMA DE DECISIONES ---
            if porcentaje_uso >= 50.0 and not estado_restringido:
                aplicar_restriccion(porcentaje_uso)
                estado_restringido = True
                
            elif porcentaje_uso < 10.0 and estado_restringido:
                restaurar_capacidad()
                estado_restringido = False
            
        except Exception:
            pass
            
        time.sleep(4)

if __name__ == "__main__":
    monitorear_cluster()