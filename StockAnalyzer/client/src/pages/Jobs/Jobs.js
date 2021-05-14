import React, { Component } from 'react'

import Paper from '@material-ui/core/Paper';
import Table from '@material-ui/core/Table';
import TableHead from '@material-ui/core/TableHead';
import TableBody from '@material-ui/core/TableBody';
import TableRow from '@material-ui/core/TableRow';
import TableCell from '@material-ui/core/TableCell';
import Job from 'pages/Jobs/Job';

export class Jobs extends Component {
    constructor(props){
        super(props);
        this.state = { 
            userList : [],
            jobs: '',
            completed: 0
        };
    } 

    render() {
        // const cellList = ["번호", "인터페이스 이름", "실행", "시작일", "최근실행시간", "주기", "설정"]
        const cellList = ["번호", "인터페이스 이름", "실행"]
        const jobs     = [{"id" : 1, "jobname" : "[EBEST] 종목코드 가져오기"},
                          {"id" : 2, "jobname" : "[EBEST] 종목가격정보 가져오기"},
                          {"id" : 3, "jobname" : "[EBEST] 모든 종목정보 가져오기"},
                          {"id" : 4, "jobname" : "[EBEST] 일자별 주가정보 가져오기(거래량 증가 종목 찾기)"},
                          {"id" : 5, "jobname" : "[EBEST] 매출, 영업이익 순위 가져오기"}]
        return (
            <div>
                <Paper>
                <Table >
                    <TableHead>
                        <TableRow>
                            {cellList.map((c, index) => {return <TableCell key={index}>{c}</TableCell>})}
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {
                            jobs.map((c, index) => {
                                return <Job key={index} id={c.id} jobname = {c.jobname} />
                            })
                        }
                    </TableBody>
                </Table>
                </Paper>
            </div>
        )
    }
}

export default Jobs
