from PIL import Image

try:
    img = Image.open('static/loading.gif')
    duration = 0
    frame = 0
    
    try:
        while True:
            duration += img.info.get('duration', 100)
            frame += 1
            img.seek(frame)
    except EOFError:
        pass
    
    print(f'Total duration: {duration}ms ({duration/1000}s) across {frame} frames')
except ImportError:
    print("PIL/Pillow not installed. Trying alternative method...")
    import os
    size = os.path.getsize('static/loading.gif')
    print(f"GIF file size: {size} bytes")
    print("Cannot determine exact duration without PIL. Typical loading GIFs are 2-4 seconds.")
except Exception as e:
    print(f"Error reading GIF: {e}")
