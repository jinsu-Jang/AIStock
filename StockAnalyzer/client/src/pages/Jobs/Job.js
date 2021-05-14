import React, { Component } from 'react'
import TableRow from '@material-ui/core/TableRow';
import TableCell from '@material-ui/core/TableCell';
import Button from '@material-ui/core/Button';
import Config from "../../config.json";

export class Job extends Component {

    callJob(id){
        const url = 'http://127.0.0.1:5000/job/' + id;
        fetch(url, {
            method: 'GET'
        })
        .then(res => res.json())
        .then(data => {
            console.log(data)
            alert(data.errmsg);
        });
 
    }
    render() {
        return (
            <TableRow>
                <TableCell>{this.props.id}</TableCell>
                <TableCell>{this.props.jobname}</TableCell>

                <TableCell><Button variant="contained" color="primary" onClick={(e) => {this.callJob(this.props.id)}}>실행</Button></TableCell>
        </TableRow>
        )
    }
}

export default Job

