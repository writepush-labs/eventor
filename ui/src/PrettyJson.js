import React from 'react';

export default ({src}) => {
    return (
        <pre style={{
            backgroundColor: '#1f4662',
            color: '#fff',
            fontSize: '12px',
            padding: "15px",
            borderRadius: "2px"
        }}>{JSON.stringify(JSON.parse(src), null, 2)}</pre>
    )
}