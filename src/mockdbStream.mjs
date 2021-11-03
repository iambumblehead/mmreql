import stream from 'stream';

const mockdbStreamReadable = ( docs, isEmptyEnd, curIndex = 0 ) => new stream.Readable({
  objectMode: true,
  read () {
    if ( curIndex < docs.length ){
      var data = docs[ curIndex ];

      curIndex = curIndex + 1;

      this.push( data );
    } else if ( isEmptyEnd ) {
      this.push( null ); // ends the stream, emits 'end'
    }
  }
    
});

const mockdbStream = ( docs, isEmptyEnd, isChanges, isTypes ) => {
  const streamReadable = mockdbStreamReadable( docs, isEmptyEnd );

  streamReadable.each = async ( fn, onFinish ) => {
    for await ( const doc of streamReadable ) {
      fn( null, doc );
    }

    if ( typeof onFinish === 'function' )
      onFinish();
  };

  streamReadable.next = async () => new Promise( async ( resolve, reject ) => {
    const value = await streamReadable.read();

    // 'type' is info returned to some cursors: 'add', 'update', 'inital' etc
    if ( value && ( 'new_val' in value || 'old_val' in value ) && !isTypes ) {
      delete value.type;
    }

    return value === null
      ? reject( new Error( 'No more rows in the cursor.' ) )
      : resolve( isChanges ? value : value.new_val );
  });

  return streamReadable;
};

export default mockdbStream;
