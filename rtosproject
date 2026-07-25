import asyncio
from yolo_uno import *
from pins import *
from lcd1602 import *
from dht20 import *

# Hardware initialization 
led_D13 = Pins(D13_PIN)
rgb_led_D3 = RGBLed(D3_PIN, 4) # Heater LED
rgb_led_D5 = RGBLed(D5_PIN, 4) # Cooler LED
rgb_led_D7 = RGBLed(D7_PIN, 4) # Humidifier LED

lcd1602 = LCD1602()
dht20 = DHT20()

# Configuration thresholds
TEMP_COOLER_THRESHOLD = 30.0   # Activate Cooler above this threshold
HUMI_DRY_THRESHOLD = 60.0      # Activate Humidifier below this threshold 

# Using Queues (Lists) and Semaphores instead of bare Global Variables
MAX_ITEMS = 5
cooler_queue = []
heater_queue = []
humidifier_queue = []

# Independent Semaphores for each actuator task to prevent deadlock/starvation
sem_cooler = asyncio.Semaphore(0)
sem_heater = asyncio.Semaphore(0)
sem_humidifier = asyncio.Semaphore(0)

# Task 1: Blinky Task (System Heartbeat - Toggles LED D13 every 1 second independently)
async def task_LED_Blinky():
    while True:
        led_D13.toggle()
        await asleep_ms(1000)

# Task 2: Read Temperature Task (Producer - Reads sensor every 5s and pushes data to Queues)
async def task_read_sensor():
    while True:
        await asleep_ms(5000) 
        temp = await dht20.atemperature()
        humi = await dht20.ahumidity()
        
        print("TEMP:", temp, "Â°C  |  HUMI:", humi, "%")
        
        # Update LCD 16x2 display 
        lcd1602.clear()
        lcd1602.show(f"TEMP: {temp} C", 0, 0)
        lcd1602.show(f"HUMI: {humi} %", 1, 0)
        
        # Pack data into a Dictionary object 
        sensor_data = {'temperature': temp, 'humidity': humi}
        
        # Push to queues if not full and release semaphores to signal consumers
        if len(cooler_queue) < MAX_ITEMS:
            cooler_queue.append(sensor_data)
            sem_cooler.release()
            
        if len(heater_queue) < MAX_ITEMS:
            heater_queue.append(sensor_data)
            sem_heater.release()
            
        if len(humidifier_queue) < MAX_ITEMS:
            humidifier_queue.append(sensor_data)
            sem_humidifier.release()

# Task 3: Cooler Task (Consumer - FSM with 5s fixed duration and stale data clearing)
async def task_cooler():
    state = 'Idle'
    current_data = None
    while True:
        if state == 'Idle':
            await sem_cooler.acquire()
            current_data = cooler_queue.pop(0)
            
            if current_data['temperature'] >= TEMP_COOLER_THRESHOLD:
                state = 'Cooling'
            else:
                for i in range(4): rgb_led_D5.show(i, hex_to_rgb('#000000')) # Turn OFF
                
        elif state == 'Cooling':
            print("Cooling for 5 seconds...")
            for i in range(4): rgb_led_D5.show(i, hex_to_rgb('#00ff00')) # GREEN
            await asleep_ms(5000) # Fixed duration cooling cycle as required by proposal
            
            # Clear stale data accumulated during the 5s sleep, keeping only the latest sample
            while len(cooler_queue) > 1:
                await sem_cooler.acquire()
                cooler_queue.pop(0)
                
            # If queue is empty, wait for the sensor task to push a new sample before evaluating
            await sem_cooler.acquire()
            current_data = cooler_queue.pop(0)
            
            # Re-check the latest actual temperature
            if current_data['temperature'] < TEMP_COOLER_THRESHOLD:
                print("Temp normal. Turning off Cooler.")
                for i in range(4): rgb_led_D5.show(i, hex_to_rgb('#000000'))
                state = 'Idle'
            else:
                print("Temp still high! Continue cooling...")
                # Keep state = 'Cooling' to repeat the 5s cycle

# Task 4: Heater Task (Consumer - Evaluates temperature against safety thresholds)
async def task_heater():
    while True:
        await sem_heater.acquire()
        data = heater_queue.pop(0)
        temp = data['temperature']

        # Classify into 3 safety/risk zones
        if temp < 20.0:
            # RED (Critical - Too Cold)
            for i in range(4):
                rgb_led_D3.show(i, hex_to_rgb('#ff0000'))

        elif 20.0 <= temp < 26.0:
            # ORANGE (Warning)
            for i in range(4):
                rgb_led_D3.show(i, hex_to_rgb('#ff8000'))

        elif 26.0 <= temp < 30.0:
            # GREEN (Safe)
            for i in range(4):
                rgb_led_D3.show(i, hex_to_rgb('#00ff00'))

        else:
            # OFF (Temperature >= 30Â°C, Cooler takes over)
            for i in range(4):
                rgb_led_D3.show(i, hex_to_rgb('#000000'))
                
# Task 5: Humidifier Task (Consumer - Complex sequential FSM: Green 5s -> Yellow 3s -> Red 2s)
async def task_humidifier():
    state = 'Idle'
    current_data = None
    while True:
        if state == 'Idle':
            await sem_humidifier.acquire()
            current_data = humidifier_queue.pop(0)
            
            if current_data['humidity'] < HUMI_DRY_THRESHOLD:
                print("Low humidity! Starting humidifier cycle...")
                state = 'State_green'
            else:
                for i in range(4): rgb_led_D7.show(i, hex_to_rgb('#000000')) # Turn OFF
                
        elif state == 'State_green':
            for i in range(4): rgb_led_D7.show(i, hex_to_rgb('#00ff00')) # GREEN for 5s
            await asleep_ms(5000)
            state = 'State_yellow'
            
        elif state == 'State_yellow':
            for i in range(4): rgb_led_D7.show(i, hex_to_rgb('#ffff00')) # YELLOW for 3s
            await asleep_ms(3000)
            state = 'State_red'
            
        elif state == 'State_red':
            for i in range(4): rgb_led_D7.show(i, hex_to_rgb('#ff0000')) # RED for 2s
            await asleep_ms(2000)
            
            # A full humidification cycle takes 10s, accumulating old samples in the queue.
            # Clear stale data, retaining only the single latest sensor reading.
            while len(humidifier_queue) > 1:
                await sem_humidifier.acquire()
                humidifier_queue.pop(0)
            
            # If queue is empty, wait for the sensor task to provide a fresh sample
            await sem_humidifier.acquire()
            current_data = humidifier_queue.pop(0)
            
            # Re-check actual humidity after the 10s cycle
            if current_data['humidity'] < HUMI_DRY_THRESHOLD:
                print("-Humidity still low! Repeating cycle...")
                state = 'State_green' # Repeat the sequence
            else:
                print("-Humidity reached target. Turning off Humidifier.")
                for i in range(4): rgb_led_D7.show(i, hex_to_rgb('#000000'))
                state = 'Idle'

# SETUP & MAIN LOOP
async def setup():
    print('App started')
    create_task(task_LED_Blinky())
    create_task(task_read_sensor())
    create_task(task_heater())
    create_task(task_cooler())
    create_task(task_humidifier())

async def main():
    await setup()
    while True:
        await asyncio.sleep_ms(100)

run_loop(main())
