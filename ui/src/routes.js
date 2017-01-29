import React from "react";
import { Router, Route, Link, IndexRoute, IndexRedirect } from "react-router";
import { useRouterHistory } from 'react-router';
import { createHistory } from 'history';
import Container from './Container'
import StreamsPage from './pages/Streams'
import StreamPage from './pages/Stream'

class Routes extends React.Component {
    render() {
        return (
            <Router history={useRouterHistory(createHistory)({ basename: '/' })}>
                <Route path="/" component={Container} name='Home'>
                    <IndexRedirect to="streams" />
                    <Route path="streams" component={StreamsPage} name='Streams'/>
                    <Route path="streams/:name" component={StreamPage} name='Stream'/>
                </Route>
            </Router>
        )
    }
}

export default Routes;

