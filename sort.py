import sys, os, array, heapq, tempfile, multiprocessing

def int_batches(filepath, limit):
    with open(filepath, encoding='utf-8') as f:
        buf, batch = "", []
        while True:
            chunk = f.read(1 << 20)
            if not chunk:
                break
            buf += chunk
            last = len(buf) - 1
            while last >= 0 and not buf[last].isspace():
                last -= 1
            complete = buf[:last]
            buf = buf[last:]
            for token in complete.split():
                try:
                    batch.append(int(token))
                except ValueError:
                    continue
                if len(batch) >= limit:
                    yield batch
                    batch = []
        for token in buf.split():
            try:
                batch.append(int(token))
            except ValueError:
                continue
            if len(batch) >= limit:
                yield batch
                batch = []
        if batch:
            yield batch

def sort_chunk(batch, tmp_dir):
    arr = array.array('q', sorted(batch))   # 这里必须用 'q'
    fd, name = tempfile.mkstemp('.tmp', dir=tmp_dir)
    with os.fdopen(fd, 'wb') as f:
        f.write(arr.tobytes())
    return name

def iter_temp(filepath):
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(16384)
            if not data:
                break
            arr = array.array('q')
            arr.frombytes(data)
            yield from arr

def merge_files(temp_files, output):
    with open(output, 'w', encoding='utf-8') as out:
        for v in heapq.merge(*(iter_temp(tf) for tf in temp_files)):
            out.write(str(v) + '\n')
    for tf in temp_files:
        try: os.remove(tf)
        except: pass

def main():
    if len(sys.argv) != 3:
        print("用法: python sort.py <文本文件名> <一次最多装载的整数个数>")
        sys.exit(1)
    input_file, limit = sys.argv[1], int(sys.argv[2])
    if not os.path.isfile(input_file):
        sys.exit("文件不存在")
    tmp_dir = tempfile.mkdtemp()
    out_file = os.path.join(os.path.dirname(input_file) or '.', '输出.txt')

    with multiprocessing.Pool() as pool:
        tasks = [pool.apply_async(sort_chunk, (b, tmp_dir))
                 for b in int_batches(input_file, limit)]
        if not tasks:
            open(out_file, 'w').close()
            sys.exit(0)
        chunks = [t.get() for t in tasks]

    merge_files(chunks, out_file)
    os.rmdir(tmp_dir)
    print(f"完成，结果保存在 {out_file}")

if __name__ == '__main__':
    main()