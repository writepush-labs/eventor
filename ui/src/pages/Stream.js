import React from 'react';
import {Card, CardActions, CardHeader, CardTitle, CardText} from 'material-ui/Card';
import {Table, TableBody, TableHeader, TableHeaderColumn, TableRow, TableRowColumn} from 'material-ui/Table';
import ClearFix from 'material-ui/internal/ClearFix';
import spacing from 'material-ui/styles/spacing';
import axios from 'axios';
import forOwn from 'lodash/forOwn';
import PrettyJson from '../PrettyJson'
import Chip from 'material-ui/Chip';
import IconCalendar from 'material-ui/svg-icons/action/date-range';
import Avatar from 'material-ui/Avatar';

const styles = {
    chip: {
        margin: 4,
    },
    wrapper: {
        display: 'flex',
        flexWrap: 'wrap',
    },
};


class Stream extends React.Component {
    componentDidMount() {
        axios.get(`http://localhost:9400/introspect/${this.props.routeParams.name}`).then(({data}) => {
            this.setState({
                stream: data
            })
        });
    }

    render() {
        if ( ! this.state) return null;

        let { stream } = this.state;

        let eTypes = [];

        if (stream && stream.event_variants) {
            forOwn(stream.event_variants, (e, eType) =>
                eTypes.push(<ClearFix key={eType}>
                    <Card style={{marginBottom: spacing.desktopGutter }}>
                        <CardHeader
                            title={eType}
                            subtitle={`${e.length} variant(s)`}
                        >
                        </CardHeader>
                        <CardText>
                            {e.map((variant, i) =>
                                <div key={i} style={{marginBottom: 32}}>
                                    <div style={styles.wrapper} key={i}>
                                        <Chip style={styles.chip}>
                                            <Avatar size={32}>P</Avatar>
                                            {variant.position}
                                        </Chip>
                                        <Chip style={styles.chip}>
                                            {variant.uuid}
                                        </Chip>
                                        <Chip style={styles.chip}>
                                            <Avatar color="#444" icon={<IconCalendar />} />
                                            {variant.created}
                                        </Chip>
                                    </div>
                                    <PrettyJson src={variant.body} />
                                </div>
                            )}
                        </CardText>
                    </Card>
                </ClearFix>)
            )
        }

        return (
            <div>
                <h2>{stream.name}</h2>
                {eTypes}
            </div>
        )
    }
}

export default Stream;