import struct

def get_gif_duration(filepath):
    """Read GIF file and calculate total duration from frame delays."""
    try:
        with open(filepath, 'rb') as f:
            # Check GIF signature
            if f.read(6) not in (b'GIF87a', b'GIF89a'):
                return None
            
            # Skip logical screen descriptor
            f.read(7)
            
            # Check for global color table
            flags = struct.unpack('B', f.read(1))[0]
            f.seek(-1, 1)
            
            total_duration = 0
            frame_count = 0
            
            while True:
                try:
                    block = f.read(1)
                    if not block:
                        break
                    
                    if block == b'\x21':  # Extension
                        label = f.read(1)
                        if label == b'\xf9':  # Graphic Control Extension
                            f.read(1)  # block size
                            f.read(1)  # packed fields
                            delay = struct.unpack('<H', f.read(2))[0]  # delay time in 1/100 sec
                            total_duration += delay * 10  # convert to ms
                            frame_count += 1
                        # Skip rest of extension
                        while True:
                            block_size = struct.unpack('B', f.read(1))[0]
                            if block_size == 0:
                                break
                            f.read(block_size)
                    elif block == b'\x2c':  # Image descriptor
                        f.read(9)  # skip image descriptor
                        # Skip local color table if present
                        flags = struct.unpack('B', f.read(1))[0]
                        f.seek(-1, 1)
                        # Skip image data
                        f.read(1)  # LZW minimum code size
                        while True:
                            block_size = struct.unpack('B', f.read(1))[0]
                            if block_size == 0:
                                break
                            f.read(block_size)
                    elif block == b'\x3b':  # Trailer
                        break
                except:
                    break
            
            return total_duration, frame_count
    except Exception as e:
        return None

result = get_gif_duration('static/loading.gif')
if result:
    duration_ms, frames = result
    duration_sec = duration_ms / 1000
    print(f"Loading GIF duration: {duration_ms}ms ({duration_sec}s)")
    print(f"Frame count: {frames}")
    print(f"\nRecommended timeout for loading.html: {duration_ms}ms")
else:
    print("Could not read GIF duration. Using default 2500ms.")
