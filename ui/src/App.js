import React, { Component } from 'react';
import './App.css';
import injectTapEventPlugin from 'react-tap-event-plugin';
import MuiThemeProvider from 'material-ui/styles/MuiThemeProvider';
import Routes from './routes';

injectTapEventPlugin();

export default () => {
    return (
        <MuiThemeProvider>
            <Routes />
        </MuiThemeProvider>
    );
}