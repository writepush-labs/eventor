import React from 'react';
import {Card, CardActions, CardHeader, CardTitle, CardText} from 'material-ui/Card';
import {Table, TableBody, TableHeader, TableHeaderColumn, TableRow, TableRowColumn} from 'material-ui/Table';
import FlatButton from 'material-ui/FlatButton';
import ClearFix from 'material-ui/internal/ClearFix';
import spacing from 'material-ui/styles/spacing';
import axios from 'axios';
import { Link } from 'react-router';
import Field from 'material-ui/TextField';
import filter from 'lodash/filter';

class Streams extends React.Component {
    componentDidMount() {
        axios.get(`http://localhost:9400/introspect`).then(({data}) => {
            this.setState({
                original: data,
                streams: data
            })
        });
    }

    searchStreams = (el) => {
        this.setState({
            streams: filter(this.state.original, v => v.name.toLowerCase().indexOf(el.target.value.toLowerCase()) != -1)
        });
    }

    render() {
        if ( ! this.state) return null;

        let { streams } = this.state;

        return (
            <div>
                <h2>Streams</h2>
                <Field fullWidth={true} hintText="Search" onChange={this.searchStreams} />
                { ! streams && <h4>No streams found</h4>}
                {streams && streams.sort((a, b) => a.name > b.name ).map((stream, i) =>
                    <ClearFix key={i}>
                        <Card style={{marginBottom: spacing.desktopGutter }}>
                            <CardHeader
                                title={stream.name}
                                subtitle={`${stream.total} events`}
                            >
                                <Link to={`/streams/${stream.name}`}><FlatButton label="Explore" primary={true} style={{float: "right"}}/></Link>
                            </CardHeader>
                            <CardText>
                                <Table>
                                    <TableHeader displaySelectAll={false} adjustForCheckbox={false}>
                                        <TableRow>
                                            <TableHeaderColumn>Event type</TableHeaderColumn>
                                            <TableHeaderColumn>Count</TableHeaderColumn>
                                        </TableRow>
                                    </TableHeader>
                                    <TableBody displayRowCheckbox={false}>
                                        {stream.event_types.map((type, ii) =>
                                            <TableRow key={ii}>
                                                <TableRowColumn>{type.name}</TableRowColumn>
                                                <TableRowColumn>{type.total}</TableRowColumn>
                                            </TableRow>
                                        )}
                                    </TableBody>
                                </Table>
                            </CardText>
                        </Card>
                    </ClearFix>
                )}
            </div>
        )
    }
}

export default Streams;