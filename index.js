const Rx = require('rx');
const Immutable = require('immutable');
const readline = require('readline');
const fs = require('fs');
const path = require('path');

const identity = x => x;
const topicSink = new Rx.Subject();
const topics = topicSink
    .scan(
        (map, topic) =>
            map.has(topic) ? map : map.set(topic, new LogTopic(topic)),
        Immutable.Map()
    )
    .replay(1)

topics.connect();
topics.subscribe();


const fileSystemWatcher = (directory) => Rx.Observable.create(observer => {
    const watcher = fs.watch(directory, (event, filename) => observer.onNext({
        event: event,
        filename: filename,
        directory: directory,
        path: path.join(directory, filename)
    }));
    return () => watcher.close();
});

const exists = (filename) => Rx.Observable.fromCallback(fs.exists, fs)(filename);

const existence = (filename) => Rx.Observable.just(0).concat(
        fileSystemWatcher(path.dirname(filename))
            .filter(ev => ev.path === filename)
    )
    .flatMap(_ => exists(filename))
    .distinctUntilChanged();

/** Returns an observable of all lines in the file, and all lines
 later emitted to the same file */
const readLog = (filename) => {
    topicSink.onNext(filename);
    return topics
        .find(map => map.has(filename))
        .map(map => map.get(filename))
        .flatMap(topic => topic.source);
};

/** Returns an observable that, while subscribed to, will emit lines
 from stream into the file, and any subscriber of that file */
const writeLog = (filename, stream) => {
    topicSink.onNext(filename);
    return topics
        .find(map => map.has(filename))
        .map(map => map.get(filename))
        .flatMap(topic =>
            stream.doOnNext(line => {
                if (typeof line !== 'string') throw new Error('Non-string emitted to log stream.');
                else if (line.indexOf('\n') !== -1) throw new Error('Line break emitted to log stream.');
                else topic.sink.onNext(line)
            })
        );
}

const readLinesFromFile = (filename) => Rx.Observable.create((observer) => {
    const reader = readline.createInterface({
        input: fs.createReadStream(filename)
    })
        .on('line', line => observer.onNext(line))
        .on('close', () => observer.onCompleted());
    return () => reader.close();
})

function LogTopic(filename) {
    this.sink = new Rx.Subject();
    const buffer = this.sink.pausableBuffered();
    this.source =
        existence(filename)
            .filter(identity)
            .take(1)
            .flatMap(readLinesFromFile(filename))
            .doOnCompleted(() => {
                buffer.resume()
                console.log("Done")
            })
            .concat(buffer);

    this.sink
        .doOnNext(line => fs.appendFile(
            filename,
            line + '\n'
        ), err => this.sink.onError(err))
    .subscribe();
}

module.exports = {
    topics: topics.map(identity),
    readLog: readLog,
    writeLog: writeLog
};
